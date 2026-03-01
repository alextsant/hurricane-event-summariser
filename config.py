"""
config.py — Centralised constants, verified NHC endpoints, and env loading.

NHC RSS documentation:  https://www.nhc.noaa.gov/aboutrss.shtml
NHC GIS documentation:  https://www.nhc.noaa.gov/gis/
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── API Keys ────────────────────────────────────────────────────────────────
MISTRAL_API_KEY     = os.getenv("MISTRAL_API_KEY", "")
BRAVE_API_KEY       = os.getenv("BRAVE_API_KEY", "")
ELEVENLABS_API_KEY  = os.getenv("ELEVENLABS_API_KEY", "")
ELEVENLABS_VOICE_ID = os.getenv("ELEVENLABS_VOICE_ID", "pNInz6obpgDQGcFmaJgB")

POLL_INTERVAL_MINUTES = int(os.getenv("POLL_INTERVAL_MINUTES", 60))

# ── NHC RSS Feeds ────────────────────────────────────────────────────────────
# Docs: https://www.nhc.noaa.gov/aboutrss.shtml
# Each basin supports up to 5 simultaneous named/numbered storms.
NHC_RSS_PATTERNS = {
    "atlantic":        "https://www.nhc.noaa.gov/nhc_at{n}.xml",
    "eastern_pacific": "https://www.nhc.noaa.gov/nhc_ep{n}.xml",
    "central_pacific": "https://www.nhc.noaa.gov/nhc_cp{n}.xml",
}
NHC_MAX_STORMS_PER_BASIN = 5

# Graphical Tropical Weather Outlooks (2-day & 5-day)
# Docs: https://www.nhc.noaa.gov/aboutrss.shtml
NHC_OUTLOOK_FEEDS = {
    "atlantic":        "https://www.nhc.noaa.gov/gtwo_at.xml",
    "eastern_pacific": "https://www.nhc.noaa.gov/gtwo_ep.xml",
    "central_pacific": "https://www.nhc.noaa.gov/gtwo_cp.xml",
}

# ── NHC GIS Shapefile URLs ────────────────────────────────────────────────────
# Docs: https://www.nhc.noaa.gov/gis/
# Active storms — primary path: storm_graphics/{UPPER}/{UPPER}_suffix.zip
NHC_GIS_STORM_GRAPHICS_BASE = "https://www.nhc.noaa.gov/storm_graphics/"
# Historical archive (advisory number required): archive/{id}_{adv:03d}_suffix.zip
NHC_GIS_BASE = "https://www.nhc.noaa.gov/gis/forecast/archive/"

NHC_GIS_SUFFIXES = {
    "cone_polygon":     "_5day_pgn.zip",  # 5-day cone of uncertainty polygon
    "track_points":     "_5day_pts.zip",  # 5-day track forecast points
    "track_line":       "_5day_lin.zip",  # 5-day track forecast line
    "watches_warnings": "_ww_wwlin.zip",  # Watch/Warning zones
}

# NOAA storm surge shapefile (included in NHC GIS during active surge threats)
# Pattern: {storm_id}_surge_{location}.zip — fetched dynamically from advisory
NHC_SURGE_GIS_SUFFIX = "_surge_inundation.zip"

# ── External APIs ─────────────────────────────────────────────────────────────
# Brave Search docs: https://api.search.brave.com/app/documentation/web-search/get-started
BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"
# Nominatim docs:  https://nominatim.org/release-docs/develop/api/Search/
NOMINATIM_URL    = "https://nominatim.openstreetmap.org/search"
# Nominatim ToS requires a real contact email in the User-Agent — set NOMINATIM_EMAIL in .env
_nominatim_email = os.getenv("NOMINATIM_EMAIL", "")
NOMINATIM_UA     = f"HurricaneTrackerHackathon/1.0 ({_nominatim_email})" if _nominatim_email else "HurricaneTrackerHackathon/1.0"

# ── Mistral Models ────────────────────────────────────────────────────────────
# Docs: https://docs.mistral.ai/getting-started/models/
MISTRAL_SUMMARY_MODEL = "mistral-large-latest"   # Most capable, use sparingly
MISTRAL_FAST_MODEL    = "mistral-small-latest"   # Cheaper for quick queries
MISTRAL_MAX_TOKENS    = 1024

# ── Cache / Rate Limiting ─────────────────────────────────────────────────────
REQUEST_TIMEOUT_S = 15
NHC_CACHE_TTL_S   = 3600  # 60 min; matches POLL_INTERVAL_MINUTES so cache stays valid between polls
BRAVE_CACHE_TTL_S = 3600  # 1 h
