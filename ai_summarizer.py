"""
ai_summarizer.py — Mistral API integration for hurricane summaries.

Docs:
  Mistral Python SDK:  https://docs.mistral.ai/api/
  Mistral models:      https://docs.mistral.ai/getting-started/models/
  Chat completion:     https://docs.mistral.ai/capabilities/completion/
"""

from __future__ import annotations

import json
import logging
import datetime
from typing import Any

from mistralai import Mistral

from config import (
    MISTRAL_API_KEY,
    MISTRAL_SUMMARY_MODEL,
    MISTRAL_FAST_MODEL,
    MISTRAL_MAX_TOKENS,
)
from gis_processor import ThreatResult

logger = logging.getLogger(__name__)

_client: Mistral | None = None


def _get_client() -> Mistral:
    global _client
    if _client is None:
        if not MISTRAL_API_KEY:
            raise RuntimeError(
                "MISTRAL_API_KEY is not set. "
                "Add it to your .env file — see .env.example."
            )
        _client = Mistral(api_key=MISTRAL_API_KEY)
    return _client


# ─────────────────────────────────────────────────────────────────────────────
# (h) compose_mistral_context()
# ─────────────────────────────────────────────────────────────────────────────

def compose_mistral_context(
    storms: list[dict],
    rss_texts: dict[str, str],
    news: list[dict],
    threat_result: ThreatResult,
    user_location: str = "",
    historical_dt: str | None = None,
) -> dict[str, Any]:
    """
    Build a structured JSON context dict for prompting Mistral with
    all available live hurricane data.

    Args:
        storms        : list from get_active_storms()
        rss_texts     : {storm_id: advisory_text} from fetch_storm_feeds()
        news          : list from query_hurricane_news()
        threat_result : ThreatResult from is_within_threat_zone()
        user_location : original user location string

    Returns:
        Context dict ready to be JSON-serialised into a Mistral prompt.
    """
    storms_summary = [
        {
            "name":          s["name"],
            "type":          s["storm_type"],
            "basin":         s["basin"],
            "advisory_num":  s["advisory_number"],
            "published":     s["published"],
            "advisory_text": rss_texts.get(s["storm_id"], "")[:2000],  # cap length
        }
        for s in storms
    ]

    news_summary = [
        {
            "title":       n["title"],
            "description": n["description"][:300],
            "url":         n["url"],
        }
        for n in news[:5]
    ]

    return {
        "data_timestamp": historical_dt or _utcnow(),
        "historical_dt":  historical_dt,   # None for live mode
        "active_storms":  storms_summary,
        "user_location":  user_location,
        "threat_assessment": {
            "threat_level":  threat_result.threat_level,
            "in_cone":       threat_result.in_cone,
            "in_warning":    threat_result.in_warning,
            "in_watch":      threat_result.in_watch,
            "in_surge_zone": threat_result.in_surge_zone,
            "warning_type":  threat_result.warning_type,
            "distance_km":   threat_result.distance_km,
            "summary":       threat_result.threat_summary,
        },
        "latest_news": news_summary,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Prompt templates & Mistral API calls
# ─────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = (
    "You are HurricaneAI, an expert meteorologist and emergency management advisor. "
    "You receive live hurricane data and produce clear, calm, accurate summaries "
    "for the public. Be concise (under 300 words), factual, and always end with "
    "3 specific safety actions the user should take right now based on their threat level. "
    "Never speculate beyond the provided data. "
    "Only discuss storms that are directly threatening or near the user's location — "
    "do not mention distant storms in other ocean basins that pose no threat to them. "
    "Always write measurements and coordinates in plain spoken language so the text "
    "reads naturally when converted to speech: write 'knots' not 'kt', "
    "'millibars' not 'mb', 'miles per hour' not 'mph', and express coordinates as "
    "full words (e.g. '17.5 degrees North, 76.8 degrees West') — never use degree "
    "symbols (°), compass abbreviations (N/S/E/W alone), or unit abbreviations."
)


def generate_hurricane_summary(context: dict[str, Any]) -> str:
    """
    Call Mistral Large to produce a natural-language hurricane summary.

    Args:
        context: Output of compose_mistral_context()

    Returns:
        Natural-language summary string.

    Approximate cost: ~800 input tokens + ~400 output tokens per call
                      ≈ 0.003 € at mistral-large-latest pricing
    Docs: https://docs.mistral.ai/capabilities/completion/
    """
    context_json  = json.dumps(context, indent=2)
    location      = context.get("user_location") or "the user's location"
    threat_level  = context["threat_assessment"]["threat_level"]
    historical_dt = context.get("historical_dt")

    if historical_dt:
        # Historical mode: instruct Mistral to report as if it is LIVE at that moment
        user_message = (
            f"SIMULATION — the current date and time is {historical_dt}.\n"
            f"You are HurricaneAI providing a LIVE public briefing right now at {historical_dt}. "
            f"Speak entirely in the present tense. Do NOT say 'historical', 'past', "
            f"'backtest', or 'no longer'. Treat every storm as actively ongoing.\n\n"
            f"Here is the current hurricane data as of {historical_dt}:\n\n"
            f"```json\n{context_json}\n```\n\n"
            f"Please produce:\n"
            f"1. A 2-sentence situation overview of all active storms.\n"
            f"2. A specific threat assessment for the user at {location}.\n"
            f"3. Three immediate safety actions (numbered list) tailored to their "
            f'threat level of "{threat_level}".'
        )
    else:
        user_message = (
            f"Here is the live hurricane data as of {context['data_timestamp']}:\n\n"
            f"```json\n{context_json}\n```\n\n"
            f"Please produce:\n"
            f"1. A 2-sentence situation overview of all active storms.\n"
            f"2. A specific threat assessment for the user at {location}.\n"
            f"3. Three immediate safety actions (numbered list) tailored to their "
            f'threat level of "{threat_level}".'
        )

    client   = _get_client()
    response = client.chat.complete(
        model=MISTRAL_SUMMARY_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_message},
        ],
        max_tokens=MISTRAL_MAX_TOKENS,
        temperature=0.3,  # Low temperature for factual accuracy
    )
    return response.choices[0].message.content

def _utcnow() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
