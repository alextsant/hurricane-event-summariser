"""
geocoder.py — Free geocoding via OpenStreetMap Nominatim.

Docs: https://nominatim.org/release-docs/develop/api/Search/
ToS:  https://operations.osmfoundation.org/policies/nominatim/
      (max 1 request/sec; must set a meaningful User-Agent)
"""

from __future__ import annotations

import time
import logging
import requests

from config import NOMINATIM_URL, NOMINATIM_UA, REQUEST_TIMEOUT_S

logger = logging.getLogger(__name__)
_last_nominatim_call: float = 0.0


# ─────────────────────────────────────────────────────────────────────────────
# (f) geocode_user_location()
# ─────────────────────────────────────────────────────────────────────────────

def geocode_user_location(address_or_postcode: str) -> dict:
    """
    Convert a free-text address or postcode to latitude/longitude using
    the OpenStreetMap Nominatim API (no API key required).

    Args:
        address_or_postcode: e.g. "Miami, FL", "90210", "Kingston, Jamaica"

    Returns:
        Dict with:
            success      : bool
            lat          : float or None
            lon          : float or None
            display_name : str  — human-readable resolved address

    Docs: https://nominatim.org/release-docs/develop/api/Search/
    """
    global _last_nominatim_call

    if not address_or_postcode or not address_or_postcode.strip():
        return {"success": False, "lat": None, "lon": None, "display_name": ""}

    # Enforce Nominatim ToS: max 1 request per second
    elapsed = time.time() - _last_nominatim_call
    if elapsed < 1.0:
        time.sleep(1.0 - elapsed)

    params = {
        "q":      address_or_postcode.strip(),
        "format": "json",
        "limit":  1,
    }
    headers = {"User-Agent": NOMINATIM_UA}

    try:
        resp = requests.get(
            NOMINATIM_URL,
            params=params,
            headers=headers,
            timeout=REQUEST_TIMEOUT_S,
        )
        _last_nominatim_call = time.time()
        resp.raise_for_status()
        results = resp.json()
    except Exception as exc:
        logger.error(f"Nominatim error for '{address_or_postcode}': {exc}")
        return {"success": False, "lat": None, "lon": None, "display_name": ""}

    if not results:
        logger.warning(f"Nominatim returned no results for '{address_or_postcode}'")
        return {"success": False, "lat": None, "lon": None, "display_name": ""}

    best = results[0]
    return {
        "success":      True,
        "lat":          float(best["lat"]),
        "lon":          float(best["lon"]),
        "display_name": best.get("display_name", ""),
    }
