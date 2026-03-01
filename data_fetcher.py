"""
data_fetcher.py — Live NHC + NOAA storm data fetching.

Official NHC RSS docs:    https://www.nhc.noaa.gov/aboutrss.shtml
Official NHC GIS docs:    https://www.nhc.noaa.gov/gis/
NOAA P-Surge info:        https://www.nhc.noaa.gov/surge/psurge.php
"""

from __future__ import annotations

import io
import os
import re
import time
import tempfile
import zipfile
import datetime
import logging
import concurrent.futures
from typing import Optional

import feedparser
import requests
import geopandas as gpd

from config import (
    NHC_RSS_PATTERNS,
    NHC_MAX_STORMS_PER_BASIN,
    NHC_GIS_BASE,
    NHC_GIS_STORM_GRAPHICS_BASE,
    NHC_GIS_SUFFIXES,
    NHC_OUTLOOK_FEEDS,
    NHC_SURGE_GIS_SUFFIX,
    BRAVE_SEARCH_URL,
    BRAVE_API_KEY,
    REQUEST_TIMEOUT_S,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# TTL Cache — avoids repeat HTTP calls within advisory windows
# ─────────────────────────────────────────────────────────────────────────────

class _TTLCache:
    """Simple time-to-live in-memory cache."""

    def __init__(self, ttl_seconds: int):
        self._store: dict = {}
        self._ttl = ttl_seconds

    def get(self, key: str):
        if key in self._store:
            value, ts = self._store[key]
            if time.time() - ts < self._ttl:
                return value
            del self._store[key]
        return None

    def set(self, key: str, value) -> None:
        self._store[key] = (value, time.time())


_advisory_cache = _TTLCache(ttl_seconds=1800)  # 30 min
_news_cache     = _TTLCache(ttl_seconds=3600)  # 1 hr


# ─────────────────────────────────────────────────────────────────────────────
# (a) get_active_storms()
# ─────────────────────────────────────────────────────────────────────────────

def get_active_storms() -> list[dict]:
    """
    Poll all NHC basin RSS feeds and return a list of currently active
    tropical cyclones.

    Each storm dict contains:
        storm_id       : str  — e.g. 'al052024'  (ATCF-style, lowercase)
        storm_id_upper : str  — e.g. 'AL052024'
        name           : str  — e.g. 'HELENE'
        storm_type     : str  — e.g. 'Hurricane', 'Tropical Storm'
        basin          : str  — 'atlantic' | 'eastern_pacific' | 'central_pacific'
        advisory_number: str
        rss_url        : str
        advisory_url   : str
        published      : str
        summary_text   : str  — first RSS item description

    Docs: https://www.nhc.noaa.gov/aboutrss.shtml
    """
    active: list[dict] = []
    year = datetime.datetime.utcnow().year
    basin_codes = {
        "atlantic":        "al",
        "eastern_pacific": "ep",
        "central_pacific": "cp",
    }

    for basin, pattern in NHC_RSS_PATTERNS.items():
        for n in range(1, NHC_MAX_STORMS_PER_BASIN + 1):
            url = pattern.format(n=n)
            try:
                feed = feedparser.parse(url)
                # feedparser sets bozo=True on malformed/empty feeds
                if not feed.entries or feed.get("bozo_exception"):
                    continue
                entry = feed.entries[0]
                parsed = _parse_storm_entry(
                    entry=entry,
                    basin=basin,
                    rss_url=url,
                    slot=n,
                    year=year,
                    basin_code=basin_codes[basin],
                )
                if parsed:
                    active.append(parsed)
                    logger.info(f"Active storm: {parsed['name']} ({parsed['storm_id']})")
            except Exception as exc:
                logger.debug(f"Slot {basin}:{n} — no storm or error: {exc}")

    return active


def _parse_storm_entry(
    entry: dict,
    basin: str,
    rss_url: str,
    slot: int,
    year: int,
    basin_code: str,
) -> Optional[dict]:
    """Extract structured metadata from one NHC RSS feed entry."""
    title = entry.get("title", "")
    if not title or "Advisory" not in title:
        return None

    # Match storm type and name: "Hurricane HELENE Advisory Number 12"
    type_name_re = re.search(
        r"(Tropical\s+(?:Storm|Depression)|Subtropical\s+(?:Storm|Depression)"
        r"|Hurricane|Post-Tropical\s+Cyclone)\s+([A-Z][A-Z\-]+)",
        title,
        re.IGNORECASE,
    )
    storm_type = type_name_re.group(1).strip() if type_name_re else "Tropical Cyclone"
    storm_name = type_name_re.group(2).upper() if type_name_re else f"STORM{slot:02d}"

    adv_match = re.search(r"Advisory\s+Number\s+(\d+[A-Z]?)", title, re.IGNORECASE)
    advisory_number = adv_match.group(1) if adv_match else "001"

    storm_id = f"{basin_code}{slot:02d}{year}"

    return {
        "storm_id":        storm_id,
        "storm_id_upper":  storm_id.upper(),
        "name":            storm_name,
        "storm_type":      storm_type,
        "basin":           basin,
        "advisory_number": advisory_number,
        "rss_url":         rss_url,
        "advisory_url":    entry.get("link", ""),
        "published":       entry.get("published", ""),
        "summary_text":    entry.get("summary", ""),
    }


# ─────────────────────────────────────────────────────────────────────────────
# (b) fetch_storm_feeds()
# ─────────────────────────────────────────────────────────────────────────────

def fetch_storm_feeds(storm_meta: dict) -> dict:
    """
    Download and parse all available RSS text for a given active storm.

    Args:
        storm_meta: One dict from get_active_storms()

    Returns:
        Dict with keys:
            advisory_text : full text of latest public advisory
            all_entries   : list of all RSS entry dicts for this storm
            gis_links     : list of GIS/shapefile URLs found in descriptions
            raw_feed      : the raw feedparser result

    Docs: https://www.nhc.noaa.gov/aboutrss.shtml
    """
    url = storm_meta["rss_url"]

    cached = _advisory_cache.get(url)
    if cached:
        logger.debug(f"Advisory cache hit: {url}")
        return cached

    feed = feedparser.parse(url)
    entries = []
    gis_links: list[str] = []

    for entry in feed.entries:
        text = entry.get("summary", "") or entry.get("description", "")
        entries.append({
            "title":     entry.get("title", ""),
            "link":      entry.get("link", ""),
            "text":      text,
            "published": entry.get("published", ""),
        })
        # Harvest any embedded GIS/shapefile/KML URLs from HTML descriptions
        found = re.findall(
            r"https://www\.nhc\.noaa\.gov/gis/[^\s\"\'<>]+\.(?:zip|kml|kmz)",
            text,
            re.IGNORECASE,
        )
        gis_links.extend(found)

    advisory_text = entries[0]["text"] if entries else ""

    result = {
        "advisory_text": advisory_text,
        "all_entries":   entries,
        "gis_links":     list(set(gis_links)),
        "raw_feed":      feed,
    }
    _advisory_cache.set(url, result)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# (c) parse_hurricane_gis()
# ─────────────────────────────────────────────────────────────────────────────

def parse_hurricane_gis(storm_meta: dict) -> dict[str, Optional[gpd.GeoDataFrame]]:
    """
    Fetch and parse NHC GIS shapefile assets for a storm into GeoDataFrames.

    Tries the standard NHC GIS archive URL pattern for each layer type.
    Falls back to any GIS links found in the RSS description.

    Args:
        storm_meta: One dict from get_active_storms()

    Returns:
        Dict keyed by layer name ('cone_polygon', 'track_points',
        'track_line', 'watches_warnings') → GeoDataFrame or None

    Docs: https://www.nhc.noaa.gov/gis/
    """
    storm_id       = storm_meta["storm_id"]        # e.g. "al052024"
    storm_id_upper = storm_meta["storm_id_upper"]  # e.g. "AL052024"
    results: dict[str, Optional[gpd.GeoDataFrame]] = {}

    for layer_name, suffix in NHC_GIS_SUFFIXES.items():
        gdf = None

        # 1st choice: storm_graphics/{UPPER}/{UPPER}_suffix.zip — correct path for active storms
        live_url = f"{NHC_GIS_STORM_GRAPHICS_BASE}{storm_id_upper}/{storm_id_upper}{suffix}"
        gdf = _fetch_shapefile_from_zip_url(live_url)

        if gdf is None:
            # 2nd choice: archive path, lowercase then uppercase (some servers are case-sensitive)
            for variant in [storm_id, storm_id_upper]:
                url = f"{NHC_GIS_BASE}{variant}{suffix}"
                gdf = _fetch_shapefile_from_zip_url(url)
                if gdf is not None:
                    break

        results[layer_name] = gdf
        if gdf is not None:
            logger.info(f"Loaded GIS layer '{layer_name}' for {storm_id}: {len(gdf)} features")
        else:
            logger.debug(f"GIS layer '{layer_name}' not available for {storm_id}")

    return results


def _fetch_shapefile_from_zip_url(url: str) -> Optional[gpd.GeoDataFrame]:
    """
    Download a zipped shapefile from a URL and return it as a GeoDataFrame.
    Returns None if the URL is unavailable or parsing fails.
    """
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT_S)
        if resp.status_code != 200:
            return None
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        shp_names = [n for n in zf.namelist() if n.endswith(".shp")]
        if not shp_names:
            return None
        # Write all shapefile components to a temp dir for geopandas to read
        with tempfile.TemporaryDirectory() as tmpdir:
            zf.extractall(tmpdir)
            shp_path = os.path.join(tmpdir, shp_names[0])
            gdf = gpd.read_file(shp_path)
        return gdf
    except Exception as exc:
        logger.debug(f"Could not load shapefile from {url}: {exc}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# (d) fetch_noaa_storm_surge()
# ─────────────────────────────────────────────────────────────────────────────

def fetch_noaa_storm_surge(storm_meta: Optional[dict] = None) -> dict:
    """
    Fetch storm surge data for an active storm.

    NHC integrates storm surge forecasts into the standard advisory products.
    This function:
      1. Extracts storm surge text from the advisory RSS (always available).
      2. Attempts to load the storm surge inundation shapefile from NHC GIS
         if the storm is active (available during named storms with surge risk).

    Args:
        storm_meta: Storm dict from get_active_storms(), or None.

    Returns:
        Dict with:
            surge_text : str  — surge paragraph extracted from advisory
            surge_gdf  : GeoDataFrame or None — inundation polygons

    Docs: https://www.nhc.noaa.gov/surge/
          https://www.nhc.noaa.gov/gis/
    """
    surge_text = ""
    surge_gdf  = None

    if storm_meta:
        feeds    = fetch_storm_feeds(storm_meta)
        advisory = feeds.get("advisory_text", "")

        # Extract storm surge section from advisory text
        surge_match = re.search(
            r"(STORM SURGE[^\n]*\n(?:[^\n]+\n?)+?)(?:\n\n|\Z)",
            advisory,
            re.IGNORECASE,
        )
        if surge_match:
            surge_text = surge_match.group(1).strip()
        else:
            # Fallback: grab any sentence mentioning surge
            surge_sentences = re.findall(
                r"[^.]*(?:storm surge|surge|inundation)[^.]*\.",
                advisory,
                re.IGNORECASE,
            )
            surge_text = " ".join(surge_sentences[:5])

        # Try to load storm surge inundation shapefile from NHC GIS
        storm_id = storm_meta["storm_id"]
        for variant in [storm_id, storm_id.upper()]:
            url = f"{NHC_GIS_BASE}{variant}{NHC_SURGE_GIS_SUFFIX}"
            gdf = _fetch_shapefile_from_zip_url(url)
            if gdf is not None:
                surge_gdf = gdf
                logger.info(f"Loaded surge inundation GIS for {storm_id}")
                break

    return {
        "surge_text": surge_text,
        "surge_gdf":  surge_gdf,
    }


# ─────────────────────────────────────────────────────────────────────────────
# (e) query_hurricane_news()
# ─────────────────────────────────────────────────────────────────────────────

def query_hurricane_news(
    query: str,
    count: int = 5,
    before_date: Optional[str] = None,
) -> list[dict]:
    """
    Query Brave Web Search API for news about a hurricane.

    Args:
        query:       Search string, e.g. "Hurricane Helene Jamaica latest"
        count:       Number of results to return (1–20)
        before_date: Optional ISO date string "YYYY-MM-DD". When set, the
                     search targets articles published up to and including
                     that date (historical backtest mode).

    Returns:
        List of dicts with keys: title, url, description, published

    Docs: https://api.search.brave.com/app/documentation/web-search/get-started
    Cost: ~$0.005 per query on free tier ($5 credit ≈ 1000 queries)
    """
    if not BRAVE_API_KEY:
        logger.warning("BRAVE_API_KEY not set; skipping news fetch.")
        return []

    cache_key = f"news:{query}:{count}:{before_date}"
    cached = _news_cache.get(cache_key)
    if cached:
        logger.debug(f"News cache hit: {query}")
        return cached

    headers = {
        "Accept":               "application/json",
        "Accept-Encoding":      "gzip",
        "X-Subscription-Token": BRAVE_API_KEY,
    }

    if before_date:
        # Historical mode — no server-side freshness filter (custom date ranges
        # require a paid Brave tier and silently return 0 results on free plans).
        # Instead, fetch extra results and filter client-side by page_age so that
        # only articles published in the 2-day window up to before_date are kept.
        # Requesting count*4 gives a large enough pool after filtering.
        params = {
            "q":                query,
            "count":            min(count * 4, 20),
            "text_decorations": False,
            "search_lang":      "en",
        }
    else:
        # Live mode — "past week" covers the last several days up to now, which
        # is the best approximation of "a couple of days" on Brave's free tier
        # (pd=1 day is too narrow; pw=7 days is the next available step).
        params = {
            "q":                query,
            "count":            min(count, 20),
            "freshness":        "pw",
            "text_decorations": False,
            "search_lang":      "en",
        }

    try:
        resp = requests.get(
            BRAVE_SEARCH_URL,
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT_S,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.error(f"Brave Search error: {exc}")
        return []

    raw = data.get("web", {}).get("results", [])

    if before_date:
        # Strict upper-bound filter: only keep articles whose page_age is on or
        # before before_date. Articles without a page_age are excluded — their
        # publication date is unknown so they cannot be verified as pre-event,
        # and including them risks leaking post-event knowledge.
        filtered: list[dict] = []
        for item in raw:
            page_age = item.get("page_age", "")
            if page_age and len(page_age) >= 10:
                date_str = page_age[:10]  # "YYYY-MM-DD" prefix of ISO 8601 string
                if date_str <= before_date:
                    filtered.append(item)
            # No page_age → skip: cannot confirm article is not from the future
        items = filtered[:count]
        logger.debug(
            f"Brave historical filter [* → {before_date}]: "
            f"{len(raw)} fetched, {len(filtered)} passed, {len(items)} used"
        )
    else:
        items = raw[:count]

    results = [
        {
            "title":       item.get("title", ""),
            "url":         item.get("url", ""),
            "description": item.get("description", ""),
            "published":   item.get("page_age", ""),
        }
        for item in items
    ]
    _news_cache.set(cache_key, results)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# (f) filter_live_news()
# ─────────────────────────────────────────────────────────────────────────────

def filter_live_news(news: list[dict], timeout: int = 4) -> list[dict]:
    """
    Remove news items whose URLs return an HTTP error or don't respond.

    Each URL is probed with a HEAD request (falling back to a streaming GET
    if the server doesn't support HEAD). All probes run in parallel so the
    added latency is bounded by the slowest single response, not the total.

    Args:
        news    : List of dicts with at minimum a 'url' key.
        timeout : Per-request timeout in seconds (default 4).

    Returns:
        Filtered list containing only items whose URL returned HTTP < 400.
    """
    if not news:
        return news

    def _is_live(item: dict) -> bool:
        url = item.get("url", "")
        if not url:
            return False
        try:
            resp = requests.head(url, timeout=timeout, allow_redirects=True)
            if resp.status_code == 405:
                # Server doesn't support HEAD — use GET but don't download body
                resp = requests.get(url, timeout=timeout, stream=True)
                resp.close()
            return resp.status_code < 400
        except Exception:
            return False

    workers = min(len(news), 8)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        live_flags = list(executor.map(_is_live, news))

    kept   = sum(live_flags)
    total  = len(news)
    logger.debug(f"URL validation: {kept}/{total} news links are live")
    return [item for item, ok in zip(news, live_flags) if ok]


# ─────────────────────────────────────────────────────────────────────────────
# Bonus: fetch_outlook_feeds() — always available, even off-season
# ─────────────────────────────────────────────────────────────────────────────

def fetch_outlook_feeds() -> dict[str, str]:
    """
    Fetch the 2-day graphical tropical weather outlook text for all basins.
    Always available regardless of active storm count.

    Returns:
        Dict mapping basin name → outlook summary text.

    Docs: https://www.nhc.noaa.gov/aboutrss.shtml
    """
    results: dict[str, str] = {}
    for basin, url in NHC_OUTLOOK_FEEDS.items():
        try:
            feed = feedparser.parse(url)
            results[basin] = feed.entries[0].summary if feed.entries else "No data available."
        except Exception as exc:
            logger.warning(f"Could not fetch outlook for {basin}: {exc}")
            results[basin] = "Unavailable."
    return results
