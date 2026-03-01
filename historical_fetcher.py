"""
historical_fetcher.py — NHC ATCF best track data for historical backtesting.

Allows the app to replay any past storm from the perspective of a specific
date and time, fetching the NHC forecast GIS that was current at that moment.

Data sources:
  ATCF best track directory : https://ftp.nhc.noaa.gov/atcf/btk/
  NHC GIS archive           : https://www.nhc.noaa.gov/gis/forecast/archive/

ATCF best track format reference:
  https://www.nrlmry.navy.mil/atcf_web/docs/database/new/abrdeck.html
"""

from __future__ import annotations

import re
import logging
import concurrent.futures
from datetime import datetime
from typing import Optional

import requests
import geopandas as gpd

from config import REQUEST_TIMEOUT_S, NHC_GIS_BASE
from data_fetcher import _fetch_shapefile_from_zip_url

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
ATCF_BTK_BASE = "https://ftp.nhc.noaa.gov/atcf/btk/"

_BASIN_MAP = {
    "al": "atlantic",
    "ep": "eastern_pacific",
    "cp": "central_pacific",
}

_STORM_TYPE_LABEL = {
    "DB": "Disturbance",
    "TD": "Tropical Depression",
    "TS": "Tropical Storm",
    "TY": "Typhoon",
    "TC": "Tropical Cyclone",
    "HU": "Hurricane",
    "SD": "Subtropical Depression",
    "SS": "Subtropical Storm",
    "EX": "Extratropical Cyclone",
    "PT": "Post-tropical Cyclone",
    "LO": "Low",
    "WV": "Tropical Wave",
    "IN": "Inland",
}

# GIS archive suffixes — same as live fetch, but with advisory number inserted
_HIST_GIS_SUFFIXES = {
    "cone_polygon":     "_5day_pgn.zip",
    "track_points":     "_5day_pts.zip",
    "track_line":       "_5day_lin.zip",
    "watches_warnings": "_ww_wwlin.zip",
}

_ATCF_UA = {"User-Agent": "HurricaneTrackerHackathon/1.0 (historicalbacktest)"}

# Session-level caches — ATCF best-track data for past years is immutable,
# so caching avoids redundant HTTP requests on repeated button presses.
_best_track_cache: dict[str, list[dict]] = {}
_storm_list_cache: dict[int, list[dict]] = {}


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def get_storms_at_datetime(target_dt: datetime) -> list[dict]:
    """
    Return all NHC-tracked storms that were active at `target_dt` (UTC naive).

    Each returned dict has the same keys as get_active_storms() plus:
        best_track         : list[dict]  — all parsed ATCF rows
        position_at_target : dict        — interpolated lat/lon/wind at target_dt

    Args:
        target_dt: UTC naive datetime for the historical query

    Returns:
        List of storm dicts, empty if no storms were active.
    """
    year   = target_dt.year
    storms = _list_storms_for_year(year)
    active = []

    for storm in storms:
        bt = storm["best_track"]
        if not bt:
            continue
        first_dt = bt[0]["dt"]
        last_dt  = bt[-1]["dt"]
        if not (first_dt <= target_dt <= last_dt):
            continue

        adv_num  = _estimate_advisory_number(bt, target_dt)
        position = _interpolate_position(bt, target_dt)

        storm["advisory_number"]    = adv_num
        storm["position_at_target"] = position
        active.append(storm)

    return active


def fetch_historical_gis(
    storm_id: str, advisory_num: int
) -> dict[str, Optional[gpd.GeoDataFrame]]:
    """
    Fetch NHC archived forecast GIS shapefiles for a specific advisory.

    Tries the exact advisory number, then ±1, ±2, ±3 as fallbacks in case
    the file for the exact advisory wasn't archived.

    Args:
        storm_id:     Lowercase ATCF storm ID, e.g. "al052025"
        advisory_num: Advisory number to fetch (1-indexed)

    Returns:
        Dict keyed by layer name → GeoDataFrame or None.
        Same format as parse_hurricane_gis().
    """
    # Try candidate advisory numbers in order of proximity to the estimate.
    # Range is ±15 because the 6-hour interval assumption can drift for long
    # storms or when NHC issues special advisories.
    offsets    = sorted(range(-15, 16), key=abs)  # [0, -1, 1, -2, 2, ..., ±15]
    candidates = [advisory_num + d for d in offsets if advisory_num + d > 0]

    def _fetch_one_layer(
        layer_key: str, suffix: str
    ) -> tuple[str, Optional[gpd.GeoDataFrame]]:
        """Search for one GIS layer across all candidate advisory numbers."""
        for adv in candidates:
            # NHC archive uses both lowercase and uppercase storm IDs — try both
            for variant in [storm_id, storm_id.upper()]:
                url = f"{NHC_GIS_BASE}{variant}_{adv:03d}{suffix}"
                gdf = _fetch_shapefile_from_zip_url(url)
                if gdf is not None:
                    logger.info(f"Historical GIS: {storm_id} adv {adv} ({layer_key})")
                    return layer_key, gdf
        logger.debug(f"No archived GIS for {storm_id} ~adv{advisory_num} ({layer_key})")
        return layer_key, None

    # Fetch all 4 GIS layers in parallel — each layer search is independent
    layers: dict[str, Optional[gpd.GeoDataFrame]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(_fetch_one_layer, lk, sf): lk
            for lk, sf in _HIST_GIS_SUFFIXES.items()
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                layer_key, gdf = future.result()
                layers[layer_key] = gdf
            except Exception as exc:
                layer_key = futures[future]
                logger.error(f"Historical GIS layer {layer_key} error: {exc}")
                layers[layer_key] = None

    return layers


# ─────────────────────────────────────────────────────────────────────────────
# Storm discovery — ATCF best track directory + file parser
# ─────────────────────────────────────────────────────────────────────────────

def _list_storms_for_year(year: int) -> list[dict]:
    """
    Parse the ATCF best track directory to discover all storms for `year`,
    then fetch and parse each storm's best track file.

    Results are cached for the session — ATCF data for past years is immutable.
    Best-track files are fetched in parallel to reduce wall-clock time.
    """
    if year in _storm_list_cache:
        logger.debug(f"Storm list cache hit for {year}")
        return _storm_list_cache[year]

    try:
        resp = requests.get(
            ATCF_BTK_BASE, headers=_ATCF_UA, timeout=REQUEST_TIMEOUT_S
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.error(f"ATCF BTK directory unavailable: {exc}")
        return []

    # Filenames look like: bal052025.dat, bep022024.dat
    pattern = re.compile(
        rf"\bb([a-z]{{2}})(\d{{2}})({year})\.dat\b", re.IGNORECASE
    )

    # Collect all storm candidates first
    candidates: list[tuple[str, int, str]] = []
    seen: set[str] = set()
    for m in pattern.finditer(resp.text):
        basin_code = m.group(1).lower()
        storm_num  = int(m.group(2))
        storm_id   = f"{basin_code}{storm_num:02d}{year}"
        if storm_id not in seen:
            seen.add(storm_id)
            candidates.append((storm_id, storm_num, basin_code))

    # Fetch all best-track files in parallel (was sequential — major bottleneck)
    best_tracks: dict[str, list[dict]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_sid = {
            executor.submit(_get_best_track, sid): sid
            for sid, _, _ in candidates
        }
        for future in concurrent.futures.as_completed(future_to_sid):
            sid = future_to_sid[future]
            try:
                best_tracks[sid] = future.result()
            except Exception:
                best_tracks[sid] = []

    storms = []
    for storm_id, storm_num, basin_code in candidates:
        bt = best_tracks.get(storm_id, [])
        if not bt:
            continue

        last      = bt[-1]
        name      = last.get("name", "Unknown").title()
        stype_key = last.get("type", "HU")

        storms.append({
            "storm_id":        storm_id,
            "storm_id_upper":  storm_id.upper(),
            "name":            name.upper(),
            "storm_type":      _STORM_TYPE_LABEL.get(stype_key, "Tropical Cyclone"),
            "basin":           _BASIN_MAP.get(basin_code, "atlantic"),
            "advisory_number": 1,               # overridden per call
            "rss_url":         "",
            "advisory_url":    "",
            "published":       "",
            "summary_text":    "",
            "best_track":      bt,
            "first_dt":        bt[0]["dt"],
            "last_dt":         last["dt"],
        })

    logger.info(f"Found {len(storms)} storms for {year} in ATCF archive.")
    _storm_list_cache[year] = storms
    return storms


def _get_best_track(storm_id: str) -> list[dict]:
    """
    Download and parse the ATCF best track file for `storm_id`.
    Returns a list of row dicts sorted by datetime.

    Results are cached for the session — individual b-deck files are immutable
    for past storms and safe to cache indefinitely within a session.
    """
    if storm_id in _best_track_cache:
        return _best_track_cache[storm_id]

    url = f"{ATCF_BTK_BASE}b{storm_id}.dat"
    try:
        resp = requests.get(url, headers=_ATCF_UA, timeout=REQUEST_TIMEOUT_S)
        resp.raise_for_status()
    except Exception as exc:
        logger.debug(f"ATCF best track not found for {storm_id}: {exc}")
        _best_track_cache[storm_id] = []
        return []

    rows = []
    for line in resp.text.splitlines():
        row = _parse_atcf_line(line)
        if row:
            rows.append(row)
    _best_track_cache[storm_id] = rows
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# ATCF parsing
# ─────────────────────────────────────────────────────────────────────────────

def _parse_atcf_line(line: str) -> Optional[dict]:
    """
    Parse one line of an ATCF best track (b-deck) file.

    ATCF column reference (1-indexed, comma-separated):
      1  BASIN      2  CY    3  YYYYMMDDHH    5  TECH (must be BEST)
      7  LAT        8  LON   9  VMAX (kt)    10  MSLP (mb)   11  TY
     28  STORMNAME
    """
    parts = [p.strip() for p in line.split(",")]
    if len(parts) < 12:
        return None
    if parts[4].strip().upper() != "BEST":
        return None
    try:
        dt_str = parts[2].strip()
        dt = datetime(
            int(dt_str[0:4]), int(dt_str[4:6]),
            int(dt_str[6:8]), int(dt_str[8:10]),
        )
        lat      = _parse_latlon(parts[6])
        lon      = _parse_latlon(parts[7])
        wind_kt  = int(parts[8])  if parts[8].strip().lstrip("-").isdigit() else 0
        pressure = int(parts[9])  if parts[9].strip().lstrip("-").isdigit() else 0
        stype    = parts[10].strip()
        name     = parts[27].strip() if len(parts) > 27 else "Unknown"
        return {
            "dt":          dt,
            "lat":         lat,
            "lon":         lon,
            "wind_kt":     wind_kt,
            "pressure_mb": pressure,
            "type":        stype,
            "name":        name,
        }
    except (ValueError, IndexError):
        return None


def _parse_latlon(raw: str) -> float:
    """
    Convert ATCF lat/lon string to decimal degrees.

    Examples:
        "240N"  → +24.0   (tenths of degree, North)
        "750W"  → -75.0   (tenths of degree, West)
        "1200N" → +120.0  (would be unusual but parsed correctly)
    """
    raw = raw.strip()
    if not raw:
        return 0.0
    direction = raw[-1].upper()
    try:
        value = float(raw[:-1]) / 10.0
    except ValueError:
        return 0.0
    if direction in ("S", "W"):
        value = -value
    return value


# ─────────────────────────────────────────────────────────────────────────────
# Advisory number estimation + position interpolation
# ─────────────────────────────────────────────────────────────────────────────

def _estimate_advisory_number(best_track: list[dict], target_dt: datetime) -> int:
    """
    Estimate the NHC advisory number that was current at `target_dt`.

    NHC issues advisories every ~6 hours. Advisory 1 corresponds to the first
    best track entry (when NHC started tracking). Each subsequent 6-hour
    interval is approximately one advisory.
    """
    if not best_track:
        return 1
    first_dt     = best_track[0]["dt"]
    delta_hours  = (target_dt - first_dt).total_seconds() / 3600.0
    return max(1, int(delta_hours / 6) + 1)


def _interpolate_position(best_track: list[dict], target_dt: datetime) -> dict:
    """
    Linearly interpolate the storm position (lat/lon/wind) at `target_dt`.
    Falls back to the nearest entry when interpolation is not possible.
    """
    before = [r for r in best_track if r["dt"] <= target_dt]
    after  = [r for r in best_track if r["dt"] >= target_dt]

    if not before:
        return best_track[0]
    if not after:
        return best_track[-1]

    r0 = before[-1]
    r1 = after[0]

    if r0["dt"] == r1["dt"]:
        return r0

    span = (r1["dt"] - r0["dt"]).total_seconds()
    frac = (target_dt - r0["dt"]).total_seconds() / span

    return {
        "dt":          target_dt,
        "lat":         r0["lat"]  + frac * (r1["lat"]  - r0["lat"]),
        "lon":         r0["lon"]  + frac * (r1["lon"]  - r0["lon"]),
        "wind_kt":     int(r0["wind_kt"]     + frac * (r1["wind_kt"]     - r0["wind_kt"])),
        "pressure_mb": int(r0["pressure_mb"] + frac * (r1["pressure_mb"] - r0["pressure_mb"])),
        "type":        r0["type"],
        "name":        r0["name"],
    }
