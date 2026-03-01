"""
tests/test_data_fetcher.py — Tests for NHC data fetching functions.

Run:  pytest tests/ -v
"""

import pytest
import responses as resp_mock
from unittest.mock import patch

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data_fetcher import (
    get_active_storms,
    fetch_storm_feeds,
    query_hurricane_news,
    _parse_storm_entry,
)


# ── Sample RSS fixtures ──────────────────────────────────────────────────────

SAMPLE_HURRICANE_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>NHC Atlantic Tropical Weather Advisory 1</title>
    <item>
      <title>Hurricane HELENE Advisory Number 12</title>
      <link>https://www.nhc.noaa.gov/text/refresh/MIATCPAT1.shtml</link>
      <description>Dangerous storm surge expected along the Florida Gulf Coast.
        Cone shapefile: https://www.nhc.noaa.gov/gis/forecast/archive/al092024_5day_pgn.zip
      </description>
      <pubDate>Thu, 26 Sep 2024 21:00:00 +0000</pubDate>
    </item>
  </channel>
</rss>"""

SAMPLE_TROPICAL_STORM_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>NHC Atlantic Tropical Weather Advisory 2</title>
    <item>
      <title>Tropical Storm IAN Advisory Number 1</title>
      <link>https://www.nhc.noaa.gov/text/refresh/MIATCPAT2.shtml</link>
      <description>Tropical storm watches issued for the Bahamas.</description>
      <pubDate>Mon, 26 Sep 2022 15:00:00 +0000</pubDate>
    </item>
  </channel>
</rss>"""


# ── get_active_storms() ──────────────────────────────────────────────────────

class TestGetActiveStorms:

    @resp_mock.activate
    def test_returns_one_storm_when_single_feed_active(self):
        resp_mock.add(
            resp_mock.GET,
            "https://www.nhc.noaa.gov/nhc_at1.xml",
            body=SAMPLE_HURRICANE_RSS,
            status=200,
            content_type="application/xml",
        )
        # All other slots return 404 (inactive)
        for n in range(2, 6):
            resp_mock.add(
                resp_mock.GET,
                f"https://www.nhc.noaa.gov/nhc_at{n}.xml",
                status=404,
            )
        for basin in ["ep", "cp"]:
            for n in range(1, 6):
                resp_mock.add(
                    resp_mock.GET,
                    f"https://www.nhc.noaa.gov/nhc_{basin}{n}.xml",
                    status=404,
                )

        storms = get_active_storms()

        assert len(storms) == 1
        assert storms[0]["name"]           == "HELENE"
        assert storms[0]["storm_type"]     == "Hurricane"
        assert storms[0]["basin"]          == "atlantic"
        assert storms[0]["advisory_number"] == "12"
        assert "al" in storms[0]["storm_id"]

    @resp_mock.activate
    def test_returns_two_storms_from_different_basins(self):
        resp_mock.add(
            resp_mock.GET,
            "https://www.nhc.noaa.gov/nhc_at1.xml",
            body=SAMPLE_HURRICANE_RSS,
            status=200,
            content_type="application/xml",
        )
        resp_mock.add(
            resp_mock.GET,
            "https://www.nhc.noaa.gov/nhc_ep1.xml",
            body=SAMPLE_TROPICAL_STORM_RSS,
            status=200,
            content_type="application/xml",
        )
        for n in range(2, 6):
            resp_mock.add(resp_mock.GET, f"https://www.nhc.noaa.gov/nhc_at{n}.xml", status=404)
        for n in range(2, 6):
            resp_mock.add(resp_mock.GET, f"https://www.nhc.noaa.gov/nhc_ep{n}.xml", status=404)
        for n in range(1, 6):
            resp_mock.add(resp_mock.GET, f"https://www.nhc.noaa.gov/nhc_cp{n}.xml", status=404)

        storms = get_active_storms()

        assert len(storms) == 2
        basins = {s["basin"] for s in storms}
        assert "atlantic"        in basins
        assert "eastern_pacific" in basins

    @resp_mock.activate
    def test_returns_empty_list_when_no_active_storms(self):
        for basin in ["at", "ep", "cp"]:
            for n in range(1, 6):
                resp_mock.add(
                    resp_mock.GET,
                    f"https://www.nhc.noaa.gov/nhc_{basin}{n}.xml",
                    status=404,
                )
        storms = get_active_storms()
        assert storms == []


# ── _parse_storm_entry() ────────────────────────────────────────────────────

class TestParseStormEntry:

    def _make_entry(self, title, summary="", link="", published=""):
        return {"title": title, "summary": summary, "link": link, "published": published}

    def test_hurricane_entry_parsed_correctly(self):
        entry  = self._make_entry("Hurricane IAN Advisory Number 3")
        result = _parse_storm_entry(entry, "atlantic", "https://nhc/feed", 1, 2022, "al")
        assert result is not None
        assert result["name"]           == "IAN"
        assert result["storm_type"]     == "Hurricane"
        assert result["advisory_number"] == "3"
        assert result["storm_id"]       == "al012022"
        assert result["storm_id_upper"] == "AL012022"

    def test_tropical_storm_entry_parsed(self):
        entry  = self._make_entry("Tropical Storm KAREN Advisory Number 1")
        result = _parse_storm_entry(entry, "eastern_pacific", "url", 2, 2023, "ep")
        assert result is not None
        assert result["name"]       == "KAREN"
        assert "Tropical" in result["storm_type"]

    def test_missing_advisory_keyword_returns_none(self):
        entry  = self._make_entry("General Weather Outlook for the Gulf")
        result = _parse_storm_entry(entry, "atlantic", "url", 1, 2024, "al")
        assert result is None

    def test_empty_title_returns_none(self):
        entry  = self._make_entry("")
        result = _parse_storm_entry(entry, "atlantic", "url", 1, 2024, "al")
        assert result is None

    def test_advisory_number_with_letter_suffix(self):
        entry  = self._make_entry("Hurricane ALPHA Advisory Number 12A")
        result = _parse_storm_entry(entry, "atlantic", "url", 3, 2024, "al")
        assert result is not None
        assert result["advisory_number"] == "12A"


# ── fetch_storm_feeds() ──────────────────────────────────────────────────────

class TestFetchStormFeeds:

    @resp_mock.activate
    def test_extracts_gis_links_from_description(self):
        resp_mock.add(
            resp_mock.GET,
            "https://www.nhc.noaa.gov/nhc_at1.xml",
            body=SAMPLE_HURRICANE_RSS,
            status=200,
            content_type="application/xml",
        )
        storm_meta = {
            "storm_id": "al092024",
            "rss_url":  "https://www.nhc.noaa.gov/nhc_at1.xml",
        }
        result = fetch_storm_feeds(storm_meta)
        assert "advisory_text" in result
        assert isinstance(result["gis_links"], list)
        # The sample RSS contains a GIS link in the description
        assert any("al092024" in link for link in result["gis_links"])

    @resp_mock.activate
    def test_returns_empty_on_404(self):
        resp_mock.add(
            resp_mock.GET,
            "https://www.nhc.noaa.gov/nhc_at1.xml",
            status=404,
        )
        storm_meta = {
            "storm_id": "al012024",
            "rss_url":  "https://www.nhc.noaa.gov/nhc_at1.xml",
        }
        result = fetch_storm_feeds(storm_meta)
        assert result["advisory_text"] == ""
        assert result["gis_links"]     == []


# ── query_hurricane_news() ──────────────────────────────────────────────────

class TestQueryHurricaneNews:

    @resp_mock.activate
    def test_returns_article_list_on_success(self):
        resp_mock.add(
            resp_mock.GET,
            "https://api.search.brave.com/res/v1/web/search",
            json={
                "web": {
                    "results": [
                        {
                            "title":       "Hurricane Helene Threatens Florida",
                            "url":         "https://news.example.com/helene",
                            "description": "Catastrophic storm surge expected...",
                            "page_age":    "2024-09-27",
                        }
                    ]
                }
            },
            status=200,
        )
        with patch("data_fetcher.BRAVE_API_KEY", "test_key"):
            articles = query_hurricane_news("Hurricane Helene Florida", count=1)

        assert len(articles) == 1
        assert articles[0]["title"] == "Hurricane Helene Threatens Florida"
        assert articles[0]["url"]   == "https://news.example.com/helene"

    def test_returns_empty_list_without_api_key(self):
        with patch("data_fetcher.BRAVE_API_KEY", ""):
            result = query_hurricane_news("Hurricane test")
        assert result == []

    @resp_mock.activate
    def test_returns_empty_list_on_api_error(self):
        resp_mock.add(
            resp_mock.GET,
            "https://api.search.brave.com/res/v1/web/search",
            status=429,  # Rate limit
        )
        with patch("data_fetcher.BRAVE_API_KEY", "test_key"):
            result = query_hurricane_news("Hurricane test")
        assert result == []
