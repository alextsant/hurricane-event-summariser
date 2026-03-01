"""
gis_processor.py — Geospatial threat zone analysis using geopandas + shapely.

Docs:
  geopandas:  https://geopandas.org/en/stable/docs.html
  shapely:    https://shapely.readthedocs.io/en/stable/
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import geopandas as gpd
from shapely.geometry import Point

logger = logging.getLogger(__name__)


@dataclass
class ThreatResult:
    """Structured output of is_within_threat_zone()."""
    in_cone:        bool  = False
    in_warning:     bool  = False
    in_watch:       bool  = False
    in_surge_zone:  bool  = False
    warning_type:   str   = "None"   # e.g. "Hurricane Warning", "Tropical Storm Watch"
    threat_level:   str   = "None"   # "Extreme" | "High" | "Moderate" | "Low" | "None"
    threat_summary: str   = ""
    distance_km:    Optional[float] = None  # distance to nearest track point


# ─────────────────────────────────────────────────────────────────────────────
# (g) is_within_threat_zone()
# ─────────────────────────────────────────────────────────────────────────────

def is_within_threat_zone(
    lat: float,
    lon: float,
    gis_layers: dict[str, Optional[gpd.GeoDataFrame]],
    surge_gdf: Optional[gpd.GeoDataFrame] = None,
) -> ThreatResult:
    """
    Intersect a lat/lon point against NHC hazard polygons to determine
    the threat level for a location.

    Args:
        lat        : User latitude (decimal degrees, positive = North)
        lon        : User longitude (decimal degrees, negative = West)
        gis_layers : Output of parse_hurricane_gis() — dict of GeoDataFrames
        surge_gdf  : Optional surge inundation GeoDataFrame from
                     fetch_noaa_storm_surge()

    Returns:
        ThreatResult dataclass with threat flags, level, and summary.

    Docs:
        https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.contains.html
        https://shapely.readthedocs.io/en/stable/manual.html#binary-predicates
    """
    result = ThreatResult()
    point  = Point(lon, lat)  # Shapely uses (x=lon, y=lat)

    # ── Cone of Uncertainty ─────────────────────────────────────────────────
    cone_gdf = gis_layers.get("cone_polygon")
    if cone_gdf is not None and not cone_gdf.empty:
        cone_wgs84 = _ensure_wgs84(cone_gdf)
        result.in_cone = bool(cone_wgs84.geometry.contains(point).any())

    # ── Watches & Warnings ──────────────────────────────────────────────────
    ww_gdf = gis_layers.get("watches_warnings")
    if ww_gdf is not None and not ww_gdf.empty:
        ww_wgs84   = _ensure_wgs84(ww_gdf)
        containing = ww_wgs84[ww_wgs84.geometry.contains(point)]
        if not containing.empty:
            # NHC shapefile uses TCWW field for warning type classification
            ww_type_col = next(
                (c for c in containing.columns if c.upper() in ("TCWW", "STORMTYPE", "TYPE", "WARNING")),
                None,
            )
            ww_val = containing.iloc[0][ww_type_col] if ww_type_col else "Unknown"
            result.in_warning = "WARNING" in str(ww_val).upper()
            result.in_watch   = "WATCH"   in str(ww_val).upper()
            result.warning_type = str(ww_val)

    # ── Storm Surge ─────────────────────────────────────────────────────────
    if surge_gdf is not None and not surge_gdf.empty:
        surge_wgs84       = _ensure_wgs84(surge_gdf)
        result.in_surge_zone = bool(surge_wgs84.geometry.contains(point).any())

    # ── Distance to Nearest Track Point (km) ─────────────────────────────
    track_gdf = gis_layers.get("track_points")
    if track_gdf is not None and not track_gdf.empty:
        try:
            track_proj = _project_for_distance(track_gdf)
            user_gdf   = gpd.GeoDataFrame(geometry=[point], crs="EPSG:4326")
            user_proj  = user_gdf.to_crs("EPSG:3857")
            distances_m = track_proj.geometry.distance(user_proj.geometry.iloc[0])
            result.distance_km = float(distances_m.min()) / 1000.0
        except Exception as exc:
            logger.debug(f"Distance calculation failed: {exc}")

    # ── Derive Overall Threat Level ──────────────────────────────────────
    result.threat_level   = _classify_threat(result)
    result.threat_summary = _build_summary(result, lat, lon)
    return result


def _ensure_wgs84(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reproject to WGS84 (EPSG:4326) if needed."""
    if gdf.crs is None:
        return gdf.set_crs("EPSG:4326")
    if gdf.crs.to_epsg() != 4326:
        return gdf.to_crs("EPSG:4326")
    return gdf


def _project_for_distance(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reproject to EPSG:3857 (metres) for accurate distance calculation."""
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    return gdf.to_crs("EPSG:3857")


def _classify_threat(r: ThreatResult) -> str:
    if r.in_warning and r.in_surge_zone: return "Extreme"
    if r.in_warning:                      return "High"
    if r.in_watch or r.in_surge_zone:    return "Moderate"
    if r.in_cone:                         return "Low"
    return "None"


def _build_summary(r: ThreatResult, lat: float, lon: float) -> str:
    parts = [f"Location ({lat:.3f}°, {lon:.3f}°):"]
    if r.threat_level == "None":
        parts.append("No current hurricane threats detected for this location.")
        return " ".join(parts)
    parts.append(f"Threat level — {r.threat_level}.")
    if r.in_cone:       parts.append("Within 5-day cone of uncertainty.")
    if r.in_warning:    parts.append(f"Active {r.warning_type}.")
    if r.in_watch:      parts.append(f"Under {r.warning_type}.")
    if r.in_surge_zone: parts.append("In potential storm surge inundation zone.")
    if r.distance_km is not None:
        parts.append(f"Nearest forecast track point: {r.distance_km:.0f} km.")
    return " ".join(parts)
