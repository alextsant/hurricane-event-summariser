"""
key_validator.py — Console-only API key validation at app startup.

Prints a status line per key to the terminal (stdout) only.
Nothing here is wired to the Gradio UI or returned to end-users.
"""

from __future__ import annotations

import requests

from config import MISTRAL_API_KEY, BRAVE_API_KEY, ELEVENLABS_API_KEY

_W = 18   # label column width


def validate_all_keys() -> None:
    """Check all configured API keys and print a summary to the console."""
    _sep = "─" * 52
    print(f"\n{_sep}")
    print("  API Key Validation")
    print(_sep)
    _check_mistral()
    _check_brave()
    _check_elevenlabs()
    print(f"{_sep}\n")


# ── Individual validators ────────────────────────────────────────────────────

def _check_mistral() -> None:
    label = "Mistral AI".ljust(_W)
    if not MISTRAL_API_KEY:
        _fail(label, "MISTRAL_API_KEY not set")
        return
    try:
        from mistralai import Mistral
        client = Mistral(api_key=MISTRAL_API_KEY)
        client.models.list()
        _ok(label, "Valid")
    except Exception as exc:
        _fail(label, str(exc))


def _check_brave() -> None:
    label = "Brave Search".ljust(_W)
    if not BRAVE_API_KEY:
        _fail(label, "BRAVE_API_KEY not set")
        return
    try:
        resp = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers={
                "Accept":               "application/json",
                "X-Subscription-Token": BRAVE_API_KEY,
            },
            params={"q": "hurricane", "count": 1},
            timeout=10,
        )
        if resp.status_code == 200:
            _ok(label, "Valid")
        elif resp.status_code == 429:
            _ok(label, "Valid (rate-limited — quota exhausted)")
        else:
            code = resp.json().get("error", {}).get("code", f"HTTP {resp.status_code}")
            _fail(label, code)
    except Exception as exc:
        _fail(label, str(exc))


def _check_elevenlabs() -> None:
    label = "ElevenLabs TTS".ljust(_W)
    if not ELEVENLABS_API_KEY:
        _fail(label, "ELEVENLABS_API_KEY not set")
        return
    try:
        # /v1/voices is accessible with any valid key regardless of scope;
        # /v1/user requires the user_read permission which scoped keys may lack.
        resp = requests.get(
            "https://api.elevenlabs.io/v1/voices",
            headers={"xi-api-key": ELEVENLABS_API_KEY},
            timeout=10,
        )
        if resp.status_code == 200:
            voice_count = len(resp.json().get("voices", []))
            _ok(label, f"Valid  ({voice_count} voices accessible)")
        elif resp.status_code == 401:
            _fail(label, "Invalid key (401 Unauthorized)")
        else:
            _fail(label, f"HTTP {resp.status_code}")
    except Exception as exc:
        _fail(label, str(exc))


# ── Helpers ──────────────────────────────────────────────────────────────────

def _ok(label: str, detail: str) -> None:
    print(f"  {label}  [OK]   {detail}")


def _fail(label: str, detail: str) -> None:
    print(f"  {label}  [!!]   {detail}")
