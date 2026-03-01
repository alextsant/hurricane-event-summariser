"""
tests/test_ai_summarizer.py — Tests for Mistral context builder and AI calls.

Run:  pytest tests/ -v
"""

import pytest
from unittest.mock import patch, MagicMock

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from gis_processor import ThreatResult
from ai_summarizer import (
    compose_mistral_context,
    generate_hurricane_summary,
    generate_threat_explanation,
    _utcnow,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

def _sample_storms():
    return [
        {
            "name":            "HELENE",
            "storm_type":      "Hurricane",
            "basin":           "atlantic",
            "advisory_number": "5",
            "published":       "2024-09-27T21:00:00Z",
            "storm_id":        "al092024",
        }
    ]

def _sample_rss_texts():
    return {"al092024": "Dangerous Category 4 hurricane approaching Florida Gulf Coast."}

def _sample_news():
    return [
        {
            "title":       "Hurricane Helene Update",
            "description": "Category 4 surge warnings issued.",
            "url":         "https://news.example.com/helene",
        }
    ]

def _extreme_threat():
    return ThreatResult(
        in_cone=True,
        in_warning=True,
        in_surge_zone=True,
        warning_type="Hurricane Warning",
        threat_level="Extreme",
        threat_summary="Location within Hurricane Warning and surge zone.",
        distance_km=45.0,
    )

def _no_threat():
    return ThreatResult(threat_level="None", threat_summary="No threats.")


# ── compose_mistral_context() ─────────────────────────────────────────────────

class TestComposeMistralContext:

    def test_top_level_keys_present(self):
        ctx = compose_mistral_context(
            _sample_storms(), _sample_rss_texts(), _sample_news(),
            _extreme_threat(), "Tampa, FL"
        )
        for key in ("data_timestamp", "active_storms", "user_location",
                    "threat_assessment", "latest_news"):
            assert key in ctx

    def test_storm_name_preserved(self):
        ctx = compose_mistral_context(
            _sample_storms(), _sample_rss_texts(), [], _no_threat()
        )
        assert ctx["active_storms"][0]["name"] == "HELENE"

    def test_threat_assessment_mirrors_threat_result(self):
        threat = _extreme_threat()
        ctx    = compose_mistral_context(
            _sample_storms(), _sample_rss_texts(), [], threat, "Tampa"
        )
        ta = ctx["threat_assessment"]
        assert ta["threat_level"]  == "Extreme"
        assert ta["in_cone"]       is True
        assert ta["in_warning"]    is True
        assert ta["in_surge_zone"] is True
        assert ta["distance_km"]   == 45.0

    def test_advisory_text_truncated_to_2000_chars(self):
        long_text   = "X" * 5000
        rss_texts   = {"al092024": long_text}
        ctx         = compose_mistral_context(
            _sample_storms(), rss_texts, [], _no_threat()
        )
        assert len(ctx["active_storms"][0]["advisory_text"]) <= 2000

    def test_news_capped_at_five_items(self):
        news = [
            {"title": f"Article {i}", "description": "desc", "url": f"https://x.com/{i}"}
            for i in range(10)
        ]
        ctx  = compose_mistral_context(
            _sample_storms(), _sample_rss_texts(), news, _no_threat()
        )
        assert len(ctx["latest_news"]) <= 5

    def test_user_location_stored(self):
        ctx = compose_mistral_context(
            _sample_storms(), _sample_rss_texts(), [], _no_threat(), "Kingston, Jamaica"
        )
        assert ctx["user_location"] == "Kingston, Jamaica"

    def test_empty_storms_list_handled(self):
        ctx = compose_mistral_context([], {}, [], _no_threat(), "Somewhere")
        assert ctx["active_storms"] == []

    def test_timestamp_format(self):
        ts = _utcnow()
        import re
        assert re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2} UTC", ts)


# ── generate_hurricane_summary() ─────────────────────────────────────────────

class TestGenerateHurricaneSummary:

    def test_returns_string_on_success(self):
        mock_response          = MagicMock()
        mock_response.choices  = [MagicMock()]
        mock_response.choices[0].message.content = "Hurricane Helene is a Cat 4 storm."

        with patch("ai_summarizer.MISTRAL_API_KEY", "test_key"), \
             patch("ai_summarizer._client") as mock_client:
            mock_client.chat.complete.return_value = mock_response
            ctx    = compose_mistral_context(
                _sample_storms(), _sample_rss_texts(), [], _extreme_threat(), "Tampa"
            )
            result = generate_hurricane_summary(ctx)

        assert isinstance(result, str)
        assert len(result) > 0

    def test_raises_runtime_error_without_api_key(self):
        with patch("ai_summarizer.MISTRAL_API_KEY", ""), \
             patch("ai_summarizer._client", None):
            ctx = compose_mistral_context(
                _sample_storms(), _sample_rss_texts(), [], _no_threat()
            )
            with pytest.raises(RuntimeError, match="MISTRAL_API_KEY"):
                generate_hurricane_summary(ctx)


# ── generate_threat_explanation() ────────────────────────────────────────────

class TestGenerateThreatExplanation:

    def test_returns_explanation_string(self):
        mock_response         = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "A High threat means you should evacuate."

        with patch("ai_summarizer.MISTRAL_API_KEY", "test_key"), \
             patch("ai_summarizer._client") as mock_client:
            mock_client.chat.complete.return_value = mock_response
            ctx    = compose_mistral_context(
                _sample_storms(), _sample_rss_texts(), [], _extreme_threat(), "Tampa"
            )
            result = generate_threat_explanation(ctx)

        assert isinstance(result, str)
        assert len(result) > 0
