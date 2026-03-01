"""
tests/test_gis_processor.py — Tests for GIS threat zone analysis.

Run:  pytest tests/ -v
"""

import pytest
import geopandas as gpd
from shapely.geometry import Polygon, Point

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from gis_processor import (
    is_within_threat_zone,
    ThreatResult,
    _classify_threat,
    _build_summary,
    _ensure_wgs84,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

def _make_polygon_gdf(minx=-85.0, miny=20.0, maxx=-75.0, maxy=30.0, extra_cols=None):
    """Create a GeoDataFrame with a single square polygon (WGS84)."""
    poly = Polygon([
        (minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy), (minx, miny)
    ])
    data = {"geometry": [poly]}
    if extra_cols:
        data.update(extra_cols)
    return gpd.GeoDataFrame(data, crs="EPSG:4326")


def _make_point_gdf(lon=-80.0, lat=25.0):
    """Create a GeoDataFrame with a single point (WGS84)."""
    return gpd.GeoDataFrame(geometry=[Point(lon, lat)], crs="EPSG:4326")


def _empty_gis_layers():
    return {
        "cone_polygon":     None,
        "track_points":     None,
        "track_line":       None,
        "watches_warnings": None,
    }


# ── is_within_threat_zone() ──────────────────────────────────────────────────

class TestIsWithinThreatZone:

    def test_point_inside_cone_returns_low_threat(self):
        gis = {**_empty_gis_layers(), "cone_polygon": _make_polygon_gdf()}
        result = is_within_threat_zone(25.0, -80.0, gis)
        assert result.in_cone      is True
        assert result.threat_level == "Low"

    def test_point_outside_cone_returns_no_threat(self):
        gis = {**_empty_gis_layers(), "cone_polygon": _make_polygon_gdf()}
        result = is_within_threat_zone(51.5, -0.1, gis)   # London, UK
        assert result.in_cone      is False
        assert result.threat_level == "None"

    def test_all_none_layers_returns_none_threat(self):
        gis    = _empty_gis_layers()
        result = is_within_threat_zone(25.0, -80.0, gis)
        assert isinstance(result, ThreatResult)
        assert result.threat_level == "None"
        assert result.in_cone      is False
        assert result.in_warning   is False

    def test_warning_zone_returns_high_threat(self):
        ww_gdf = _make_polygon_gdf(extra_cols={"TCWW": ["Hurricane Warning"]})
        gis    = {**_empty_gis_layers(), "watches_warnings": ww_gdf}
        result = is_within_threat_zone(25.0, -80.0, gis)
        assert result.in_warning   is True
        assert result.threat_level == "High"
        assert "Warning" in result.warning_type

    def test_watch_zone_returns_moderate_threat(self):
        ww_gdf = _make_polygon_gdf(extra_cols={"TCWW": ["Hurricane Watch"]})
        gis    = {**_empty_gis_layers(), "watches_warnings": ww_gdf}
        result = is_within_threat_zone(25.0, -80.0, gis)
        assert result.in_watch     is True
        assert result.threat_level == "Moderate"

    def test_warning_plus_surge_returns_extreme_threat(self):
        ww_gdf    = _make_polygon_gdf(extra_cols={"TCWW": ["Hurricane Warning"]})
        surge_gdf = _make_polygon_gdf()
        gis       = {**_empty_gis_layers(), "watches_warnings": ww_gdf}
        result    = is_within_threat_zone(25.0, -80.0, gis, surge_gdf)
        assert result.in_warning   is True
        assert result.in_surge_zone is True
        assert result.threat_level == "Extreme"

    def test_surge_only_returns_moderate_threat(self):
        surge_gdf = _make_polygon_gdf()
        gis       = _empty_gis_layers()
        result    = is_within_threat_zone(25.0, -80.0, gis, surge_gdf)
        assert result.in_surge_zone is True
        assert result.threat_level == "Moderate"

    def test_distance_to_track_point_calculated(self):
        track_gdf = _make_point_gdf(lon=-80.5, lat=25.5)
        gis       = {**_empty_gis_layers(), "track_points": track_gdf}
        result    = is_within_threat_zone(25.0, -80.0, gis)
        assert result.distance_km is not None
        assert result.distance_km > 0
        assert result.distance_km < 200   # Miami to nearby point should be < 200 km

    def test_empty_geodataframe_does_not_raise(self):
        empty_gdf = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        gis       = {**_empty_gis_layers(), "cone_polygon": empty_gdf}
        result    = is_within_threat_zone(25.0, -80.0, gis)
        assert result.in_cone is False


# ── _classify_threat() ───────────────────────────────────────────────────────

class TestClassifyThreat:

    def test_extreme_when_warning_and_surge(self):
        r = ThreatResult(in_warning=True, in_surge_zone=True)
        assert _classify_threat(r) == "Extreme"

    def test_high_when_only_warning(self):
        r = ThreatResult(in_warning=True, in_surge_zone=False)
        assert _classify_threat(r) == "High"

    def test_moderate_when_only_watch(self):
        r = ThreatResult(in_watch=True)
        assert _classify_threat(r) == "Moderate"

    def test_moderate_when_only_surge(self):
        r = ThreatResult(in_surge_zone=True)
        assert _classify_threat(r) == "Moderate"

    def test_low_when_only_cone(self):
        r = ThreatResult(in_cone=True)
        assert _classify_threat(r) == "Low"

    def test_none_when_no_flags(self):
        r = ThreatResult()
        assert _classify_threat(r) == "None"


# ── _ensure_wgs84() ─────────────────────────────────────────────────────────

class TestEnsureWGS84:

    def test_wgs84_input_unchanged(self):
        gdf    = _make_polygon_gdf()
        result = _ensure_wgs84(gdf)
        assert result.crs.to_epsg() == 4326

    def test_reprojected_from_3857(self):
        gdf    = _make_polygon_gdf().to_crs("EPSG:3857")
        result = _ensure_wgs84(gdf)
        assert result.crs.to_epsg() == 4326

    def test_no_crs_gets_assigned_wgs84(self):
        gdf    = gpd.GeoDataFrame(geometry=[Point(0, 0)])  # No CRS
        result = _ensure_wgs84(gdf)
        assert result.crs.to_epsg() == 4326
