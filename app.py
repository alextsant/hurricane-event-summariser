"""
app.py — Gradio web interface for the Hurricane Event Summariser.

Run:  python app.py
Then open http://localhost:7860 in your browser.

Docs: https://www.gradio.app/docs/gradio/blocks
"""

from __future__ import annotations

import datetime
import logging
import math
import concurrent.futures

import gradio as gr
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import POLL_INTERVAL_MINUTES
from key_validator import validate_all_keys
from geocoder import geocode_user_location
from data_fetcher import (
    get_active_storms,
    fetch_storm_feeds,
    parse_hurricane_gis,
    fetch_noaa_storm_surge,
    query_hurricane_news,
    fetch_outlook_feeds,
    filter_live_news,
)
from gis_processor import is_within_threat_zone, ThreatResult
from ai_summarizer import compose_mistral_context, generate_hurricane_summary
from tts_handler import text_to_speech
from map_renderer import build_threat_map
from historical_fetcher import get_storms_at_datetime, fetch_historical_gis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Console-only startup check — not visible in the Gradio UI
validate_all_keys()

# ── Threat colour palette ─────────────────────────────────────────────────────
THREAT_COLOURS = {
    "None":     "#4CAF50",
    "Low":      "#CDDC39",
    "Moderate": "#FF9800",
    "High":     "#F44336",
    "Extreme":  "#9C27B0",
}
THREAT_ORDER = {"None": 0, "Low": 1, "Moderate": 2, "High": 3, "Extreme": 4}


def _estimate_positional_threat(
    lat: float, lon: float, position: dict
) -> "ThreatResult":
    """
    Fallback threat estimation when historical GIS layers are unavailable.
    Uses the Haversine distance to the storm center and its wind speed to
    classify threat level using approximate Saffir-Simpson radii.
    """
    slat    = position.get("lat", 0.0)
    slon    = position.get("lon", 0.0)
    wind_kt = position.get("wind_kt", 0)
    stype   = position.get("type", "")

    # Haversine distance (km)
    R  = 6371.0
    φ1 = math.radians(lat);  φ2 = math.radians(slat)
    Δφ = math.radians(slat - lat)
    Δλ = math.radians(slon - lon)
    a  = math.sin(Δφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(Δλ / 2) ** 2
    dist_km = 2 * R * math.asin(math.sqrt(min(a, 1.0)))

    result = ThreatResult(distance_km=dist_km)

    if wind_kt >= 64:           # Hurricane-force winds
        if dist_km < 150:
            result.threat_level = "Extreme";  result.in_warning = True;  result.in_cone = True
        elif dist_km < 350:
            result.threat_level = "High";     result.in_warning = True;  result.in_cone = True
        elif dist_km < 600:
            result.threat_level = "Moderate"; result.in_cone = True
        elif dist_km < 900:
            result.threat_level = "Low";      result.in_cone = True
    elif wind_kt >= 34:         # Tropical-storm-force winds
        if dist_km < 100:
            result.threat_level = "High";     result.in_warning = True;  result.in_cone = True
        elif dist_km < 300:
            result.threat_level = "Moderate"; result.in_cone = True
        elif dist_km < 500:
            result.threat_level = "Low";      result.in_cone = True

    note = "(Position-based estimate — archived GIS advisory data not available.)"
    if result.threat_level == "None":
        result.threat_summary = (
            f"Location ({lat:.3f}°, {lon:.3f}°): No significant threat detected. "
            f"Storm centre {dist_km:.0f} km away with {wind_kt} kt winds. {note}"
        )
    else:
        result.threat_summary = (
            f"Location ({lat:.3f}°, {lon:.3f}°): Threat level — {result.threat_level}. "
            f"Storm centre approx. {dist_km:.0f} km away, "
            f"{wind_kt} kt sustained winds ({stype}). {note}"
        )
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Background cache-warming scheduler
# Fetches active storms + advisory text every POLL_INTERVAL_MINUTES so that
# button clicks read from the TTL cache instead of waiting on NHC network I/O.
# ─────────────────────────────────────────────────────────────────────────────

def _refresh_nhc_cache() -> None:
    """Pre-warm the advisory TTL cache for all currently active storms."""
    try:
        storms = get_active_storms()
        for storm in storms:
            fetch_storm_feeds(storm)
        logger.info(
            f"NHC cache refreshed — {len(storms)} active storm(s) "
            f"at {datetime.datetime.utcnow().strftime('%H:%M UTC')}"
        )
    except Exception as exc:
        logger.warning(f"Background NHC cache refresh failed: {exc}")


_cache_scheduler = BackgroundScheduler(timezone="UTC")
_cache_scheduler.add_job(
    func=_refresh_nhc_cache,
    trigger=IntervalTrigger(minutes=POLL_INTERVAL_MINUTES),
    id="nhc_cache_refresh",
    name=f"NHC Cache Refresh (every {POLL_INTERVAL_MINUTES} min)",
    replace_existing=True,
    next_run_time=datetime.datetime.utcnow(),  # run immediately on startup
)
_cache_scheduler.start()
logger.info(f"Background NHC cache scheduler started — interval: {POLL_INTERVAL_MINUTES} min.")


# ─────────────────────────────────────────────────────────────────────────────
# Core pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run_full_analysis(
    location_input: str,
    enable_tts: bool,
    historical_mode: bool,
    historical_dt_str: str,
    progress=gr.Progress(),
):
    """
    Main Gradio handler — runs the complete hurricane analysis pipeline.

    In live mode    : fetches current NHC data.
    In historical mode : replays a past date/time using ATCF best track +
                         archived NHC GIS advisories.

    Returns:
        Tuple of (threat_badge_html, status_md, summary_text, audio_path,
                  map_html, submit_btn_update)
    """
    if not location_input.strip():
        return _empty_state("Please enter a location.")

    # ── 1. Geocode ────────────────────────────────────────────────────────
    progress(0.05, desc="Geocoding location...")
    geo = geocode_user_location(location_input)
    if not geo["success"]:
        return _empty_state(
            f"Could not geocode **'{location_input}'**. "
            "Try a more specific address, city, or postcode."
        )

    lat, lon = geo["lat"], geo["lon"]
    resolved = geo["display_name"]

    # ── 2. Resolve target datetime and storm list ─────────────────────────
    progress(0.12, desc="Fetching storm data...")
    target_dt:   datetime.datetime | None = None
    before_date: str | None               = None

    if historical_mode:
        raw = historical_dt_str.strip()
        if not raw:
            return _empty_state(
                "Historical mode is on — please enter a date and time (UTC)."
            )
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H", "%Y-%m-%d"):
            try:
                target_dt = datetime.datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
        if target_dt is None:
            return _empty_state(
                f"Could not parse **'{raw}'** as a date. "
                "Use format `YYYY-MM-DD HH:MM` (e.g. `2025-10-11 12:00`)."
            )
        if target_dt > datetime.datetime.utcnow():
            return _empty_state(
                "Historical date is in the future — please enter a past date."
            )
        before_date = target_dt.strftime("%Y-%m-%d")
        logger.info(f"Historical mode: target_dt={target_dt}, before_date={before_date}")
        storms = get_storms_at_datetime(target_dt)
    else:
        storms = get_active_storms()

    # ── 3. Handle no-storm case ───────────────────────────────────────────
    if not storms:
        if historical_mode:
            no_storm_md = (
                f"**Resolved location:** {resolved}  \n"
                f"**Coordinates:** {lat:.4f}°N, {lon:.4f}°W\n\n"
                "---\n"
                f"### No NHC-Tracked Storms on {target_dt.strftime('%Y-%m-%d %H:%M UTC')}\n"
                "No storms were being tracked by the NHC at the selected date and time.\n\n"
                "_Try a different date or use [NHC Historical Archives]"
                "(https://www.nhc.noaa.gov/archive/) to find active storms._"
            )
            no_storm_summary = (
                f"No active tropical cyclones on {target_dt.strftime('%Y-%m-%d %H:%M UTC')}. "
                f"The NHC was not tracking any storms near {location_input} at that time."
            )
        else:
            outlooks   = fetch_outlook_feeds()
            outlook_md = "\n".join(
                f"**{basin.replace('_', ' ').title()}:** {text}"
                for basin, text in outlooks.items()
            )
            no_storm_md = (
                f"**Resolved location:** {resolved}  \n"
                f"**Coordinates:** {lat:.4f}°N, {lon:.4f}°W\n\n"
                "---\n"
                "### No Active Tropical Cyclones\n"
                "The NHC is not currently tracking any named storms.\n\n"
                "**2-day Tropical Weather Outlooks:**\n\n"
                f"{outlook_md}\n\n"
                "_Monitor: [NHC Active Storms](https://www.nhc.noaa.gov/)_"
            )
            no_storm_summary = (
                "No active tropical cyclones. The NHC is not currently tracking any named storms "
                f"near {location_input}. You are safe — but please check back later, especially "
                "during hurricane season (June through November)."
            )

        no_storm_audio = None
        if enable_tts:
            no_storm_audio = text_to_speech(no_storm_summary)
        no_storm_map = build_threat_map(lat, lon, resolved, {}, [],
                                        historical_storms=storms or None)
        return (
            _badge("None"), no_storm_md, no_storm_summary,
            no_storm_audio, no_storm_map, gr.update(variant="primary"),
        )

    # ── 4. Per-storm GIS + threat evaluation ─────────────────────────────
    all_threats:   list[tuple[dict, ThreatResult]] = []
    all_rss_texts: dict[str, str]                  = {}
    all_news:      list[dict]                       = []
    all_gis:       dict[str, dict]                 = {}
    worst_threat   = ThreatResult()

    for i, storm in enumerate(storms):
        progress(
            0.25 + 0.40 * (i / max(len(storms), 1)),
            desc=f"Analysing {storm['storm_type']} {storm['name']}...",
        )
        if historical_mode:
            gis = fetch_historical_gis(storm["storm_id"], storm["advisory_number"])
            pos = storm["position_at_target"]
            advisory_text = (
                f"Advisory for {storm['storm_type']} {storm['name']} "
                f"as of {target_dt.strftime('%Y-%m-%d %H:%M UTC')}. "
                f"Current position: {pos['lat']:.2f}°N, {abs(pos['lon']):.2f}°W. "
                f"Maximum sustained winds: {pos['wind_kt']} kt. "
                f"Minimum pressure: {pos['pressure_mb']} mb. "
                f"Storm classification: {pos['type']}."
            )
        else:
            feeds = fetch_storm_feeds(storm)
            gis   = parse_hurricane_gis(storm)
            advisory_text = feeds["advisory_text"]
            surge = fetch_noaa_storm_surge(storm)

        surge_gdf = None if historical_mode else surge.get("surge_gdf")
        threat = is_within_threat_zone(lat, lon, gis, surge_gdf)

        # Historical fallback: if GIS layers are all empty, estimate from storm position
        if historical_mode and threat.threat_level == "None" and storm.get("position_at_target"):
            threat = _estimate_positional_threat(lat, lon, storm["position_at_target"])

        # Include the year in queries so Brave scopes to the right storm season
        # even without a strict date-range filter.
        year_hint = f" {target_dt.year}" if historical_mode and target_dt else ""

        # Primary query: storm near user's location
        news_query = f"Hurricane {storm['name']}{year_hint} {location_input}"

        # Impact query: damage, casualties, infrastructure — more specific
        impact_query = (
            f"Hurricane {storm['name']}{year_hint} damage casualties deaths "
            f"infrastructure {location_input}"
        )

        # Run both news queries in parallel — they are fully independent
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            f_news   = executor.submit(query_hurricane_news, news_query,   4, before_date)
            f_impact = executor.submit(query_hurricane_news, impact_query, 4, before_date)
            news        = f_news.result()
            impact_news = f_impact.result()

        # Merge, deduplicating by URL
        seen_urls = {n["url"] for n in news if n.get("url")}
        news = news + [n for n in impact_news if n.get("url") not in seen_urls]

        all_rss_texts[storm["storm_id"]] = advisory_text
        all_gis[storm["storm_id"]]        = gis
        all_news.extend(news)
        all_threats.append((storm, threat))

        if THREAT_ORDER.get(threat.threat_level, 0) > THREAT_ORDER.get(worst_threat.threat_level, 0):
            worst_threat = threat

    # Remove news items whose URLs return 404 or other errors before display
    all_news = filter_live_news(all_news)

    # ── 5. Build status markdown ──────────────────────────────────────────
    mode_header = (
        f"### Historical Backtest — {target_dt.strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        if historical_mode else
        f"### Active Storms — {len(storms)} detected\n\n"
    )

    storm_lines = []
    for s, t in all_threats:
        colour = THREAT_COLOURS.get(t.threat_level, "#9E9E9E")
        dist   = f" · Nearest track: {t.distance_km:.0f} km" if t.distance_km else ""
        adv    = s["advisory_number"]
        extra  = ""
        if historical_mode and s.get("position_at_target"):
            pos   = s["position_at_target"]
            extra = (
                f"\n- Historical position: {pos['lat']:.1f}°, {pos['lon']:.1f}° · "
                f"Wind: {pos.get('wind_kt', '?')} kt · "
                f"Pressure: {pos.get('pressure_mb', '?')} mb"
            )
        storm_lines.append(
            f"#### {s['storm_type']} {s['name']}  "
            f"<span style='color:{colour};font-weight:bold'>[{t.threat_level} Threat]</span>\n"
            f"- Basin: {s['basin'].replace('_', ' ').title()} · Advisory #{adv}"
            f"{dist}{extra}\n"
            f"- {t.threat_summary}"
        )

    news_label = (
        f"\n### News up to {target_dt.strftime('%Y-%m-%d')}\n"
        if historical_mode else "\n### Latest News\n"
    )
    news_lines = [
        f"- [{n['title']}]({n['url']})"
        for n in all_news[:4]
        if n.get("title") and n.get("url")
    ]
    news_section = (news_label + "\n".join(news_lines)) if news_lines else ""

    status_md = (
        f"**Resolved location:** {resolved}  \n"
        f"**Coordinates:** {lat:.4f}°N, {lon:.4f}°W\n\n"
        "---\n"
        + mode_header
        + "\n\n".join(storm_lines)
        + news_section
    )

    # ── 6. AI Summary ─────────────────────────────────────────────────────
    progress(0.72, desc="Generating AI briefing...")
    # Only pass storms that directly threaten the user to Mistral; distant
    # non-threatening storms stay in status_md (left panel) but clutter the briefing.
    relevant_storms = [s for s, t in all_threats if t.threat_level != "None"]
    if not relevant_storms and all_threats:
        relevant_storms = [all_threats[0][0]]   # awareness context when none are threatening

    ctx = compose_mistral_context(
        storms=relevant_storms,
        rss_texts=all_rss_texts,
        news=all_news,
        threat_result=worst_threat,
        user_location=location_input,
        historical_dt=target_dt.strftime("%Y-%m-%d %H:%M UTC") if historical_mode else None,
    )
    try:
        raw_summary = generate_hurricane_summary(ctx)
    except Exception as exc:
        raw_summary = (
            f"AI summary unavailable: {exc}\n\n"
            "Ensure MISTRAL_API_KEY is set correctly in your .env file."
        )
        logger.error(f"Mistral API error: {exc}")

    # ── Build display summary (textbox) ───────────────────────────────────
    # Historical prefix shown in textbox only (not TTS)
    display_summary = (
        f"[Historical backtest — {target_dt.strftime('%Y-%m-%d %H:%M UTC')}]\n\n"
        + raw_summary
        if historical_mode else raw_summary
    )

    # Append news links at the bottom of the textbox — NOT sent to TTS
    news_for_briefing = [n for n in all_news if n.get("title") and n.get("url")][:6]
    if news_for_briefing:
        display_summary += (
            "\n\n---\n**News & Sources** "
            "_(click to read — not included in voice briefing)_\n"
            + "\n".join(f"- [{n['title']}]({n['url']})" for n in news_for_briefing)
        )

    summary = display_summary

    # ── 7. TTS (optional) ─────────────────────────────────────────────────
    audio_path = None
    if enable_tts:
        progress(0.88, desc="Synthesising voice briefing...")
        # TTS reads raw Mistral output only — no historical prefix, no news links
        tts_text   = f"Hurricane update for {location_input}. {raw_summary}"
        audio_path = text_to_speech(tts_text)

    # ── 8. Interactive map ────────────────────────────────────────────────
    progress(0.95, desc="Rendering map...")
    map_html = build_threat_map(
        lat, lon, resolved, all_gis, storms,
        historical_storms=storms if historical_mode else None,
    )

    # Submit button colour reflects the current danger level
    threat_btn_variant = (
        "stop" if worst_threat.threat_level in ("High", "Extreme") else "primary"
    )

    return (
        _badge(worst_threat.threat_level),
        status_md,
        summary,
        audio_path,
        map_html,
        gr.update(variant=threat_btn_variant),
    )


def _badge(level: str) -> str:
    colour    = THREAT_COLOURS.get(level, "#9E9E9E")
    animation = "animation:pulse-glow 1.2s ease-in-out infinite;" if level == "Extreme" else ""
    return (
        f"<div style='background:{colour};color:white;padding:14px 24px;"
        f"border-radius:10px;font-size:1.5em;font-weight:bold;"
        f"text-align:center;letter-spacing:1px;{animation}'>"
        f"THREAT LEVEL: {level.upper()}"
        f"</div>"
    )


_MAP_PLACEHOLDER = (
    "<div style='height:560px;display:flex;align-items:center;"
    "justify-content:center;background:#f0f4f8;border-radius:8px;"
    "color:#666;font-family:sans-serif;font-size:1.05em;'>"
    "Enter a location above to view the interactive storm map."
    "</div>"
)


def _empty_state(message: str):
    return (
        _badge("None"), message, "", None,
        _MAP_PLACEHOLDER, gr.update(variant="primary"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────────────────────
_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

/* Apply Inter across the whole app */
.gradio-container, .gradio-container * {
  font-family: 'Inter', sans-serif !important;
}

/* ── Extreme threat badge — glowing pulse animation ── */
@keyframes pulse-glow {
  0%, 100% { box-shadow: 0 0  0  0 rgba(156, 39, 176, 0.55); }
  50%       { box-shadow: 0 0 22px 8px rgba(156, 39, 176, 0.22); }
}

/* ── General typography ── */
.output-markdown { font-size: 0.95em; }

/* ── Sticky app header ── */
#app-header {
  position: sticky;
  top: 0;
  z-index: 100;
  padding-bottom: 10px;
  border-bottom: 1px solid var(--border-color-primary, #dde3ee);
  margin-bottom: 6px;
  background: var(--background-fill-primary, #fff);
}

/* ── Card-style shadow on tab panels ── */
.tabitem {
  border-radius: 0 12px 12px 12px !important;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.07) !important;
  padding: 8px !important;
}

/* ── Larger touch targets for checkbox labels (iOS/Android: 44 px min) ── */
#tts-toggle label,
#hist-toggle label {
  min-height: 44px;
  display: flex !important;
  align-items: center;
  cursor: pointer;
}

/* ── Submit button — taller tap target ── */
#submit-btn { min-height: 48px; }

/* ── Map rounded corners ── */
#map-display { border-radius: 10px; overflow: hidden; }

/* ── Mobile: stack input row + shrink map ── */
@media (max-width: 768px) {
  #input-row {
    flex-wrap: wrap !important;
  }
  #input-row > * {
    min-width: 100% !important;
    flex-basis: 100% !important;
  }
  #map-display iframe {
    height: 340px !important;
  }
  .prose p,
  .output-markdown p {
    font-size: 1.05rem !important;
  }
}

/* ── Very narrow screens: scale headings down ── */
@media (max-width: 480px) {
  h1, .prose h1 { font-size: 1.4em !important; }
  h2, .prose h2 { font-size: 1.2em !important; }
  h3, .prose h3 { font-size: 1.1em !important; }
}
"""

# ─────────────────────────────────────────────────────────────────────────────
# Gradio Blocks UI
# ─────────────────────────────────────────────────────────────────────────────

with gr.Blocks(
    title="Hurricane Event Summariser",
    theme=gr.themes.Soft(
        primary_hue=gr.themes.colors.blue,
        secondary_hue=gr.themes.colors.cyan,
        neutral_hue=gr.themes.colors.slate,
    ),
    css=_CSS,
) as demo:

    # ── Header ─────────────────────────────────────────────────────────────
    gr.Markdown(
        "# Hurricane Event Summariser\n"
        "**NHC Live Data · Mistral AI · ElevenLabs TTS · Brave Search**\n\n"
        "> Enter your address or postcode to instantly check hurricane threat status "
        "and receive an AI-generated safety briefing for any active storm worldwide.",
        elem_id="app-header",
    )

    # ── Input row ─────────────────────────────────────────────────────────
    with gr.Row(elem_id="input-row"):
        with gr.Column(scale=4):
            location_box = gr.Textbox(
                label="Your Location",
                placeholder=(
                    "e.g. Miami, FL  |  Kingston, Jamaica  |  SW1A 1AA  |  "
                    "Nassau, Bahamas"
                ),
                lines=1,
            )
        with gr.Column(scale=1):
            tts_toggle = gr.Checkbox(
                label="Voice Summary (ElevenLabs TTS)",
                value=True,
                elem_id="tts-toggle",
            )
        with gr.Column(scale=1):
            submit_btn = gr.Button(
                "Check Hurricane Threat",
                variant="primary",
                scale=1,
                elem_id="submit-btn",
            )

    # ── Quick-start examples ───────────────────────────────────────────────
    gr.Examples(
        examples=[
            "Miami, FL",
            "Nassau, Bahamas",
            "Kingston, Jamaica",
            "Tampa, FL",
            "Houston, TX",
        ],
        inputs=location_box,
        label="Quick examples — click to populate",
    )

    # ── Historical backtest controls (collapsed accordion) ────────────────
    with gr.Accordion("Historical Backtest Mode (advanced)", open=False):
        historical_toggle = gr.Checkbox(
            label="Enable Historical Backtest",
            value=False,
            info="Replay a past storm to validate the app against a known event.",
            elem_id="hist-toggle",
        )
        historical_row = gr.Row(visible=False)
        with historical_row:
            historical_dt_box = gr.Textbox(
                label="Historical Date & Time (UTC)",
                placeholder="e.g. 2025-10-11 12:00",
                info=(
                    "Format: YYYY-MM-DD HH:MM  ·  "
                    "Try Hurricane Melissa (2025): 2025-10-28 17:25 with location 'Kingston, Jamaica'"
                ),
                lines=1,
                scale=3,
            )

    # ── Threat badge ──────────────────────────────────────────────────────
    threat_badge = gr.HTML(value=_badge("None"))

    # ── Output tabs ───────────────────────────────────────────────────────
    with gr.Tabs(selected=1):

        with gr.TabItem("Storm Status & News"):
            status_display = gr.Markdown(
                value="_Enter a location above and click 'Check Hurricane Threat'._",
                elem_id="status-panel",
            )

        with gr.TabItem("AI Safety Briefing"):
            with gr.Row():
                copy_btn = gr.Button(
                    "Copy briefing",
                    size="sm",
                    variant="secondary",
                    scale=0,
                )
            summary_box = gr.Markdown(
                value="_AI summary will appear here after analysis..._",
                elem_id="summary-panel",
            )
            audio_player = gr.Audio(
                label="Voice Briefing  (ElevenLabs TTS)",
                type="filepath",
                visible=True,
                autoplay=True,
            )

        with gr.TabItem("Interactive Map"):
            expand_btn = gr.Button(
                "Expand / Collapse Map",
                size="sm",
                variant="secondary",
            )
            map_display = gr.HTML(
                value=_MAP_PLACEHOLDER,
                elem_id="map-display",
            )

    # ── Footer ────────────────────────────────────────────────────────────
    gr.Markdown(
        "---\n"
        "**Data sources:** "
        "[NHC RSS Feeds](https://www.nhc.noaa.gov/aboutrss.shtml) · "
        "[NHC GIS Data](https://www.nhc.noaa.gov/gis/) · "
        "[NOAA P-Surge](https://www.nhc.noaa.gov/surge/psurge.php) · "
        "[Brave Search](https://api.search.brave.com/) · "
        "[OpenStreetMap Nominatim](https://nominatim.openstreetmap.org/)  \n"
        "**AI / TTS:** "
        "[Mistral Large](https://docs.mistral.ai/) · "
        "[ElevenLabs](https://elevenlabs.io/docs/api-reference/text-to-speech)  \n"
        "**Disclaimer:** For informational purposes only. "
        "Always follow official evacuation orders from your local emergency management authority."
    )

    # ── Event wiring ──────────────────────────────────────────────────────

    historical_toggle.change(
        fn=lambda enabled: gr.update(visible=enabled),
        inputs=[historical_toggle],
        outputs=[historical_row],
    )

    _inputs  = [location_box, tts_toggle, historical_toggle, historical_dt_box]
    _outputs = [
        threat_badge, status_display, summary_box,
        audio_player, map_display, submit_btn,
    ]

    _switch_to_briefing_js = """() => {
        const btn = Array.from(document.querySelectorAll('[role="tab"]'))
            .find(b => b.textContent.trim() === 'AI Safety Briefing');
        if (btn) btn.click();
    }"""

    submit_btn.click(
        fn=run_full_analysis,
        inputs=_inputs,
        outputs=_outputs,
        show_progress=True,
    ).then(fn=None, inputs=[], outputs=[], js=_switch_to_briefing_js)

    # Also trigger on Enter key in the location box
    location_box.submit(
        fn=run_full_analysis,
        inputs=_inputs,
        outputs=_outputs,
        show_progress=True,
    ).then(fn=None, inputs=[], outputs=[], js=_switch_to_briefing_js)

    # ── Copy AI briefing to clipboard ─────────────────────────────────────
    copy_btn.click(
        fn=None,
        inputs=[summary_box],
        outputs=[],
        js="(text) => { navigator.clipboard.writeText(text).catch(() => {}); }",
    )

    # ── Expand / collapse the Folium map iframe ───────────────────────────
    expand_btn.click(
        fn=None,
        inputs=[],
        outputs=[],
        js="""() => {
            const iframe = document.querySelector('#map-display iframe');
            if (iframe) {
                iframe.style.height = iframe.style.height === '700px' ? '560px' : '700px';
            }
        }""",
    )


if __name__ == "__main__":
    demo.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False,
        show_error=True,
    )
