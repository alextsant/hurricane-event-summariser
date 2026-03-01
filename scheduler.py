"""
scheduler.py — Background polling for newly active NHC storms.

Polls NHC RSS feeds every N minutes and logs when new storms are detected
or when a user's location threat level changes. All results are surfaced
through the Gradio UI — no notification delivery is performed here.

Docs: https://apscheduler.readthedocs.io/en/stable/
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import POLL_INTERVAL_MINUTES
from data_fetcher import get_active_storms, parse_hurricane_gis, fetch_noaa_storm_surge
from gis_processor import is_within_threat_zone, ThreatResult

logger = logging.getLogger(__name__)

_known_storm_ids: set[str] = set()
THREAT_ORDER = {"None": 0, "Low": 1, "Moderate": 2, "High": 3, "Extreme": 4}


def poll_storms(
    lat: float,
    lon: float,
    on_new_storm: Optional[Callable[[dict, ThreatResult], None]] = None,
    on_threat_change: Optional[Callable[[dict, ThreatResult], None]] = None,
) -> list[dict]:
    """
    Poll NHC for active storms and evaluate the threat for a given location.

    Args:
        lat, lon          : Geocoded coordinates (decimal degrees).
        on_new_storm      : Optional callback(storm, threat) fired when a
                            previously unseen storm is detected.
        on_threat_change  : Optional callback(storm, threat) fired when a
                            storm's threat level for this location is >= Low.

    Returns:
        List of {storm, threat} dicts for every active storm found.
    """
    global _known_storm_ids
    active  = get_active_storms()
    results: list[dict] = []

    for storm in active:
        sid    = storm["storm_id"]
        is_new = sid not in _known_storm_ids
        if is_new:
            _known_storm_ids.add(sid)
            logger.info(f"New storm detected: {storm['storm_type']} {storm['name']} ({sid})")

        gis    = parse_hurricane_gis(storm)
        surge  = fetch_noaa_storm_surge(storm)
        threat = is_within_threat_zone(lat, lon, gis, surge.get("surge_gdf"))

        if is_new and on_new_storm:
            on_new_storm(storm, threat)

        if THREAT_ORDER.get(threat.threat_level, 0) >= 1 and on_threat_change:
            on_threat_change(storm, threat)

        results.append({"storm": storm, "threat": threat})

    return results


def start_scheduler(
    lat: float,
    lon: float,
    on_new_storm: Optional[Callable] = None,
    on_threat_change: Optional[Callable] = None,
    interval_minutes: int = POLL_INTERVAL_MINUTES,
) -> BackgroundScheduler:
    """
    Start a background scheduler that polls NHC every `interval_minutes`.

    Args:
        lat, lon          : Geocoded coordinates.
        on_new_storm      : Optional callback when a new storm is detected.
        on_threat_change  : Optional callback when threat level >= Low.
        interval_minutes  : Polling frequency (default from .env).

    Returns:
        Running BackgroundScheduler. Call .shutdown() to stop it.
    """
    import datetime
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        func=poll_storms,
        trigger=IntervalTrigger(minutes=interval_minutes),
        kwargs={
            "lat":              lat,
            "lon":              lon,
            "on_new_storm":     on_new_storm,
            "on_threat_change": on_threat_change,
        },
        id="hurricane_poll",
        name="NHC Hurricane Poll",
        replace_existing=True,
        next_run_time=datetime.datetime.utcnow(),
    )
    scheduler.start()
    logger.info(f"Scheduler started — polling every {interval_minutes} min.")
    return scheduler


def stop_scheduler(scheduler: BackgroundScheduler) -> None:
    """Gracefully shut down a running scheduler."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")
