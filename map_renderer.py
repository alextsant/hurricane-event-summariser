"""
map_renderer.py — Interactive Folium map for hurricane tracking.

Renders an OpenStreetMap-based interactive map with:
  - User location pin
  - NHC 5-day cone of uncertainty polygon
  - Forecast track line and labelled track points
  - Hurricane / Tropical Storm watch & warning zones
  - Mini-map, layer control, and NHC colour legend

Folium docs: https://python-visualization.github.io/folium/
"""

from __future__ import annotations

import logging
from typing import Optional

import folium
from folium.plugins import MiniMap
import geopandas as gpd

logger = logging.getLogger(__name__)

# ── NHC standard colour conventions ──────────────────────────────────────────
_CONE_FILL    = "#FF8C00"
_CONE_BORDER  = "#CC5500"
_TRACK_COLOUR = "#8B0000"

# Track point fill by storm category abbreviation
_POINT_COLOURS: dict[str, str] = {
    "TD": "#5B9BD5",   # tropical depression — blue
    "TS": "#FFD700",   # tropical storm — gold
    "HU": "#FF4500",   # hurricane — orange-red
    "EX": "#808080",   # extratropical — grey
    "PT": "#808080",   # post-tropical — grey
    "DB": "#808080",   # disturbance — grey
    "LO": "#808080",   # low — grey
    "WV": "#808080",   # tropical wave — grey
}
_DEFAULT_PT_COLOUR = "#FF4500"

# Watch / warning fill colours
_WW_COLOURS: dict[str, str] = {
    "HU_W": "#FF0000",   # Hurricane Warning — red
    "HU_A": "#FF69B4",   # Hurricane Watch — pink
    "TR_W": "#FFD700",   # Tropical Storm Warning — yellow
    "TR_A": "#FFFF99",   # Tropical Storm Watch — light yellow
}


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def build_threat_map(
    lat: float,
    lon: float,
    location_name: str,
    all_storms_gis: dict[str, dict],   # {storm_id: gis_layers_dict}
    all_storms_meta: list[dict],        # from get_active_storms() or get_storms_at_datetime()
    historical_storms: Optional[list[dict]] = None,  # from get_storms_at_datetime(), adds best track overlay
) -> str:
    """
    Build an interactive Folium map and return it as an embeddable HTML string.

    Args:
        lat / lon          : User's geocoded coordinates (WGS-84 decimal degrees)
        location_name      : Human-readable resolved address for the popup
        all_storms_gis     : {storm_id: {cone_polygon, track_points,
                              track_line, watches_warnings}} GeoDataFrames
        all_storms_meta    : storm metadata list from get_active_storms()
        historical_storms  : Optional list from get_storms_at_datetime(). When
                             provided, adds the best track path and a position
                             marker at the selected historical moment.

    Returns:
        Self-contained HTML iframe string suitable for gr.HTML().
    """
    m = folium.Map(
        location=[lat, lon],
        zoom_start=5,
        tiles=None,          # added manually so LayerControl can toggle them
    )

    # ── Base tile layers ──────────────────────────────────────────────────
    folium.TileLayer("OpenStreetMap", name="OpenStreetMap").add_to(m)
    folium.TileLayer(
        "CartoDB positron",
        name="CartoDB Light (cleaner overlays)",
        show=False,
    ).add_to(m)
    folium.TileLayer(
        "CartoDB dark_matter",
        name="CartoDB Dark (night mode)",
        show=False,
    ).add_to(m)

    # ── Mini-map (bottom-right) ───────────────────────────────────────────
    MiniMap(toggle_display=True, position="bottomright").add_to(m)

    # ── User location pin ─────────────────────────────────────────────────
    folium.Marker(
        location=[lat, lon],
        popup=folium.Popup(
            f"<b>Your location</b><br>{location_name}", max_width=260
        ),
        tooltip="Your location",
        icon=folium.Icon(color="blue", icon="home", prefix="fa"),
    ).add_to(m)

    # ── Forecast GIS overlays (cone, track, watches/warnings) ────────────
    meta_by_id = {s["storm_id"]: s for s in all_storms_meta}

    for storm_id, gis_layers in all_storms_gis.items():
        meta  = meta_by_id.get(storm_id, {})
        label = f"{meta.get('storm_type', 'Storm')} {meta.get('name', storm_id)}"

        _add_cone(m, gis_layers.get("cone_polygon"), label)
        _add_track_line(m, gis_layers.get("track_line"), label)
        _add_track_points(m, gis_layers.get("track_points"), label)
        _add_watches_warnings(m, gis_layers.get("watches_warnings"), label)

    # ── Historical best track overlays (only in backtest mode) ───────────
    if historical_storms:
        for storm in historical_storms:
            _add_historical_best_track(m, storm)

    # ── Legend & layer control ────────────────────────────────────────────
    _add_legend(m, has_historical=bool(historical_storms))
    folium.LayerControl(collapsed=False).add_to(m)

    # ── Embed dimensions (controls the iframe rendered by _repr_html_) ────
    m.get_root().width  = "100%"
    m.get_root().height = "560px"

    return m._repr_html_()


# ─────────────────────────────────────────────────────────────────────────────
# Layer helpers
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_wgs84(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reproject to WGS-84 (EPSG:4326) if the GDF uses a different CRS."""
    if gdf.crs is None:
        return gdf.set_crs("EPSG:4326")
    if gdf.crs.to_epsg() != 4326:
        return gdf.to_crs("EPSG:4326")
    return gdf


def _add_cone(
    m: folium.Map, gdf: Optional[gpd.GeoDataFrame], label: str
) -> None:
    """Add the 5-day cone of uncertainty polygon."""
    if gdf is None or gdf.empty:
        return
    try:
        gdf = _ensure_wgs84(gdf)
        fg  = folium.FeatureGroup(name=f"{label} — 5-day Cone", show=True)
        folium.GeoJson(
            data=gdf.__geo_interface__,
            style_function=lambda _: {
                "fillColor":   _CONE_FILL,
                "color":       _CONE_BORDER,
                "weight":      1.5,
                "fillOpacity": 0.25,
                "opacity":     0.8,
            },
            tooltip=f"{label} — 5-day Cone of Uncertainty",
        ).add_to(fg)
        fg.add_to(m)
    except Exception as exc:
        logger.warning(f"Could not render cone for {label}: {exc}")


def _add_track_line(
    m: folium.Map, gdf: Optional[gpd.GeoDataFrame], label: str
) -> None:
    """Add the forecast track centre line (dashed)."""
    if gdf is None or gdf.empty:
        return
    try:
        gdf = _ensure_wgs84(gdf)
        fg  = folium.FeatureGroup(name=f"{label} — Forecast Track", show=True)
        folium.GeoJson(
            data=gdf.__geo_interface__,
            style_function=lambda _: {
                "color":      _TRACK_COLOUR,
                "weight":     2.5,
                "opacity":    0.85,
                "dashArray":  "6 3",
            },
            tooltip=f"{label} — Forecast Track Line",
        ).add_to(fg)
        fg.add_to(m)
    except Exception as exc:
        logger.warning(f"Could not render track line for {label}: {exc}")


def _add_track_points(
    m: folium.Map, gdf: Optional[gpd.GeoDataFrame], label: str
) -> None:
    """Add colour-coded forecast track points (circles, one per advisory period)."""
    if gdf is None or gdf.empty:
        return
    try:
        gdf = _ensure_wgs84(gdf)
        fg  = folium.FeatureGroup(name=f"{label} — Forecast Points", show=True)

        for _, row in gdf.iterrows():
            geom = row.geometry
            if geom is None or geom.is_empty:
                continue

            stype  = _get_col(row, ["STORMTYPE", "SSNUM", "TYPE"])
            wind   = _get_col(row, ["MAXWIND",   "WIND",  "INTENSITY"])
            tau    = _get_col(row, ["TAU",        "VALIDTIME", "DATELBL"])
            mslp   = _get_col(row, ["MSLP",       "PRESSURE"])

            key    = str(stype).upper()[:2] if stype else ""
            colour = _POINT_COLOURS.get(key, _DEFAULT_PT_COLOUR)

            lines  = [f"<b>{label}</b>"]
            if stype: lines.append(f"Type: {stype}")
            if wind:  lines.append(f"Max Wind: {wind} kt")
            if mslp:  lines.append(f"Pressure: {mslp} mb")
            if tau:   lines.append(f"Forecast: +{tau}h")

            folium.CircleMarker(
                location=[geom.y, geom.x],
                radius=7,
                color="white",
                weight=1.5,
                fill=True,
                fill_color=colour,
                fill_opacity=0.9,
                popup=folium.Popup("<br>".join(lines), max_width=200),
                tooltip=f"{label} · {wind or '?'} kt · +{tau or '?'}h",
            ).add_to(fg)

        fg.add_to(m)
    except Exception as exc:
        logger.warning(f"Could not render track points for {label}: {exc}")


def _add_watches_warnings(
    m: folium.Map, gdf: Optional[gpd.GeoDataFrame], label: str
) -> None:
    """Add watch/warning zone polygons using NHC official colours."""
    if gdf is None or gdf.empty:
        return
    try:
        gdf = _ensure_wgs84(gdf)
        fg  = folium.FeatureGroup(
            name=f"{label} — Watches/Warnings", show=True
        )

        # NHC uses various column names across advisory types
        type_col = next(
            (
                c for c in gdf.columns
                if c.upper() in ("TCWW", "ADVISTYPE", "PROD_TYPE", "WTYPE", "TYPE")
            ),
            None,
        )

        for _, row in gdf.iterrows():
            geom = row.geometry
            if geom is None or geom.is_empty:
                continue

            wtype  = str(row[type_col]).upper() if type_col else ""
            colour = _classify_ww_colour(wtype)
            tip    = str(row[type_col]) if type_col else "Watch/Warning Zone"

            single = gpd.GeoDataFrame({"geometry": [geom]}, crs=gdf.crs)
            folium.GeoJson(
                data=single.__geo_interface__,
                style_function=lambda _, c=colour: {
                    "fillColor":   c,
                    "color":       c,
                    "weight":      2,
                    "fillOpacity": 0.35,
                    "opacity":     0.85,
                },
                tooltip=f"{label} — {tip}",
            ).add_to(fg)

        fg.add_to(m)
    except Exception as exc:
        logger.warning(f"Could not render watches/warnings for {label}: {exc}")


def _classify_ww_colour(wtype: str) -> str:
    """Map an NHC watch/warning type string to its official colour."""
    if "HURRICANE" in wtype and "WARNING" in wtype:       return _WW_COLOURS["HU_W"]
    if "HURRICANE" in wtype and "WATCH"   in wtype:       return _WW_COLOURS["HU_A"]
    if "TROPICAL STORM" in wtype and "WARNING" in wtype:  return _WW_COLOURS["TR_W"]
    if "TROPICAL STORM" in wtype and "WATCH"   in wtype:  return _WW_COLOURS["TR_A"]
    # Fallback to NHC short-code format (e.g. "HU_W", "TR_A")
    for code, colour in _WW_COLOURS.items():
        if code in wtype:
            return colour
    return "#AAAAAA"


def _add_historical_best_track(m: folium.Map, storm: dict) -> None:
    """
    Add the ATCF best track path and a position marker for a historical storm.

    Draws a purple polyline from the storm's first tracked position up to the
    selected historical datetime, then places a star marker at the interpolated
    position at that moment.

    Args:
        storm: Dict from get_storms_at_datetime() — must have
               'best_track', 'position_at_target', 'name', 'storm_type'.
    """
    best_track = storm.get("best_track", [])
    position   = storm.get("position_at_target")
    if not best_track or not position:
        return

    label     = f"{storm.get('storm_type', 'Storm')} {storm.get('name', '')}"
    target_dt = position["dt"]

    # ── Best track path up to target_dt ──────────────────────────────────
    track_rows = [r for r in best_track if r["dt"] <= target_dt]
    if len(track_rows) >= 2:
        coords = [[r["lat"], r["lon"]] for r in track_rows]
        fg_track = folium.FeatureGroup(
            name=f"{label} — Best Track (actual path)", show=True
        )
        folium.PolyLine(
            locations=coords,
            color="#7B2D8B",    # purple — distinct from orange forecast cone
            weight=3,
            opacity=0.85,
            dash_array="4 2",
            tooltip=f"{label} — Verified best track up to {target_dt.strftime('%Y-%m-%d %H:%M UTC')}",
        ).add_to(fg_track)
        fg_track.add_to(m)

    # ── Storm position at the selected historical moment ──────────────────
    fg_pos = folium.FeatureGroup(
        name=f"{label} — Position at selected time", show=True
    )
    wind     = position.get("wind_kt", "?")
    pressure = position.get("pressure_mb", "?")
    stype    = position.get("type", "")
    popup_html = (
        f"<b>{label}</b><br>"
        f"<b>Historical position</b><br>"
        f"Time: {target_dt.strftime('%Y-%m-%d %H:%M UTC')}<br>"
        f"Type: {stype}<br>"
        f"Max Wind: {wind} kt<br>"
        f"Pressure: {pressure} mb"
    )
    folium.Marker(
        location=[position["lat"], position["lon"]],
        popup=folium.Popup(popup_html, max_width=220),
        tooltip=f"{label} @ {target_dt.strftime('%Y-%m-%d %H:%M')} — {wind} kt",
        icon=folium.Icon(color="purple", icon="star", prefix="fa"),
    ).add_to(fg_pos)
    fg_pos.add_to(m)


def _add_legend(m: folium.Map, has_historical: bool = False) -> None:
    """Inject a fixed-position HTML legend into the map."""
    items = [
        ("#5B9BD5", "circle", "Tropical Depression track point"),
        ("#FFD700", "circle", "Tropical Storm track point"),
        ("#FF4500", "circle", "Hurricane track point"),
        (_CONE_FILL, "square", "5-day Cone of Uncertainty"),
        (_WW_COLOURS["HU_W"], "square", "Hurricane Warning"),
        (_WW_COLOURS["HU_A"], "square", "Hurricane Watch"),
        (_WW_COLOURS["TR_W"], "square", "Tropical Storm Warning"),
        (_WW_COLOURS["TR_A"], "square", "Tropical Storm Watch"),
        ("#0078FF", "square", "Your location"),
    ]
    if has_historical:
        items.insert(3, ("#7B2D8B", "line",   "Best track (actual path)"))
        items.insert(4, ("#7B2D8B", "circle", "Storm position at selected time"))

    def _swatch(colour: str, shape: str) -> str:
        if shape == "line":
            return (
                f"<span style='display:inline-block;width:20px;height:3px;"
                f"background:{colour};opacity:0.85;margin-right:6px;"
                f"vertical-align:middle;border-radius:2px;'></span>"
            )
        radius = "50%" if shape == "circle" else "2px"
        return (
            f"<span style='background:{colour};opacity:0.85;"
            f"width:13px;height:13px;display:inline-block;"
            f"border-radius:{radius};margin-right:6px;'></span>"
        )

    rows = "".join(
        f"<div style='display:flex;align-items:center;margin:2px 0;'>"
        f"{_swatch(c, s)}"
        f"<span style='font-size:11px;'>{label}</span></div>"
        for c, s, label in items
    )

    legend_html = (
        "<div style='"
        "position:fixed;bottom:40px;left:10px;z-index:1000;"
        "background:rgba(255,255,255,0.93);padding:10px 14px;"
        "border-radius:8px;box-shadow:0 1px 6px rgba(0,0,0,0.25);"
        "font-family:sans-serif;line-height:1.6;max-width:230px;'>"
        "<div style='font-weight:bold;font-size:12px;margin-bottom:5px;'>"
        "NHC Map Legend</div>"
        f"{rows}"
        "</div>"
    )
    m.get_root().html.add_child(folium.Element(legend_html))


# ─────────────────────────────────────────────────────────────────────────────
# Utility
# ─────────────────────────────────────────────────────────────────────────────

def _get_col(row, candidates: list[str]):
    """Return the first matching column value from a GeoDataFrame row (case-insensitive)."""
    for name in candidates:
        for col in row.index:
            if col.upper() == name.upper():
                return row[col]
    return None
