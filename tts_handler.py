"""
tts_handler.py — ElevenLabs text-to-speech for hurricane summaries.

Docs:    https://elevenlabs.io/docs/api-reference/text-to-speech
SDK:     https://github.com/elevenlabs/elevenlabs-python
Free tier: 10,000 characters/month (no credit card required).

NOTE: The hackathon brief mentions 110k free credits — verify your account
tier at https://elevenlabs.io/app/subscription before production use.
The MAX_CHARS guard below can be raised if you have more credits.
"""

from __future__ import annotations

import logging
import tempfile

from config import ELEVENLABS_API_KEY, ELEVENLABS_VOICE_ID

logger = logging.getLogger(__name__)

# Cap to stay within ElevenLabs free tier (10,000 chars/month).
# A full Mistral summary is ~1,500–2,000 chars; raised from 500 to cover it.
# Lower this if you exhaust your monthly allowance.
MAX_TTS_CHARS = 2500


def text_to_speech(text: str, output_path: str | None = None) -> str | None:
    """
    Convert text to an MP3 file using ElevenLabs TTS.

    Args:
        text        : The text to synthesise. Truncated to MAX_TTS_CHARS to
                      conserve free-tier credits.
        output_path : Optional file path for the .mp3 output. If None,
                      a temporary file is created.

    Returns:
        Absolute path to the generated .mp3 file, or None on failure.

    Credit cost: 1 character = 1 credit (free tier: 10,000 chars/month)
    Docs: https://elevenlabs.io/docs/api-reference/text-to-speech
    """
    if not ELEVENLABS_API_KEY:
        logger.warning("ELEVENLABS_API_KEY not set — TTS disabled.")
        return None

    # Truncate to stay within free tier limits
    if len(text) > MAX_TTS_CHARS:
        text = text[:MAX_TTS_CHARS].rsplit(" ", 1)[0] + "..."
        logger.info(f"TTS text truncated to {MAX_TTS_CHARS} chars.")

    try:
        from elevenlabs.client import ElevenLabs
        from elevenlabs import save

        client = ElevenLabs(api_key=ELEVENLABS_API_KEY)
        audio  = client.text_to_speech.convert(
            voice_id=ELEVENLABS_VOICE_ID,
            text=text,
            model_id="eleven_multilingual_v2",
            output_format="mp3_44100_128",
        )

        if output_path is None:
            tmp         = tempfile.NamedTemporaryFile(suffix=".mp3", delete=False)
            output_path = tmp.name
            tmp.close()

        save(audio, output_path)
        logger.info(f"TTS audio saved → {output_path}")
        return output_path

    except Exception as exc:
        logger.error(f"ElevenLabs TTS failed: {exc}")
        return None
