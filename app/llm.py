"""Provider-agnostic LLM narration layer.

Every AI surface in the app (/insights, /strategy-fit) follows the same
two-layer pattern: a DETERMINISTIC brief is built from tenant-scoped
BigQuery data (no LLM, no hallucination risk), then a model NARRATES that
brief into prose. This module owns the narration call so the vendor choice
(Gemini vs Claude) lives in exactly one place.

The model never touches BigQuery or tenant scoping — it only ever sees the
pre-built text brief the caller hands it. Swapping providers therefore has
zero bearing on the multi-tenant isolation guarantees enforced upstream.

Provider selection (env `LLM_PROVIDER`):
    gemini  -> google-genai, model `GEMINI_MODEL`  (default gemini-2.0-flash)
    claude  -> anthropic,    model `CLAUDE_MODEL`  (default claude-haiku-4-5)

Both vendor SDKs are imported lazily inside their branch so the unused one
is never a hard dependency at import time.
"""
from __future__ import annotations

import logging
import os
import time as _time

from app.cost_tracking import log_cost_event

_log = logging.getLogger("happytrader.llm")

_DEFAULT_GEMINI_MODEL = "gemini-2.0-flash"
_DEFAULT_CLAUDE_MODEL = "claude-haiku-4-5"

_UNAVAILABLE = "AI is temporarily unavailable. Try again in a few minutes."
_FAILED = "Couldn't generate that right now. Try again in a moment."
_EMPTY = "The model returned an empty response. Try again in a moment."


def active_provider() -> str:
    """Return the configured provider slug ('gemini' or 'claude')."""
    return (os.environ.get("LLM_PROVIDER", "gemini") or "gemini").strip().lower()


def _gemini_model() -> str:
    return (os.environ.get("GEMINI_MODEL", "") or "").strip() or _DEFAULT_GEMINI_MODEL


def _claude_model() -> str:
    return (os.environ.get("CLAUDE_MODEL", "") or "").strip() or _DEFAULT_CLAUDE_MODEL


def _provider_api_key(provider: str) -> str:
    if provider == "claude":
        return os.environ.get("ANTHROPIC_API_KEY", "").strip()
    return os.environ.get("GEMINI_API_KEY", "").strip()


def llm_available() -> bool:
    """True when the active provider has an API key configured.

    UI surfaces gate the "Generate" button on this instead of a hardcoded
    GEMINI_API_KEY check so flipping LLM_PROVIDER=claude lights up the
    button on the Anthropic key.
    """
    return bool(_provider_api_key(active_provider()))


def call_llm(system: str, user: str, *, kind: str, max_tokens: int, temperature: float):
    """Run one narration turn against the active provider.

    system      : instructions / role (Claude's top-level system field;
                  prepended to the prompt for Gemini which has no separate slot)
    user        : the deterministic data brief to narrate
    kind        : cost-event tag, e.g. 'coach.generate' / 'coach.ask'
    max_tokens  : output cap
    temperature : sampling temperature

    Returns (text, None) on success or (None, user_facing_error). Cost is
    logged here (vendor + kind + model + duration_ms + token counts) so
    callers never repeat that bookkeeping.
    """
    if active_provider() == "claude":
        return _call_claude(system, user, kind=kind, max_tokens=max_tokens, temperature=temperature)
    return _call_gemini(system, user, kind=kind, max_tokens=max_tokens, temperature=temperature)


# --------------------------------------------------------------------
# Gemini (google-genai)
# --------------------------------------------------------------------


def _gemini_usage_fields(response) -> dict:
    """Extract token-count fields from a Gemini response, when available.

    The SDK shape varies a little across versions; we read defensively so
    a missing attribute never breaks cost logging.
    """
    out: dict = {}
    try:
        meta = getattr(response, "usage_metadata", None)
        if meta is None:
            return out
        for src, dst in (
            ("prompt_token_count", "prompt_tokens"),
            ("candidates_token_count", "output_tokens"),
            ("total_token_count", "total_tokens"),
        ):
            v = getattr(meta, src, None)
            if v is not None:
                out[dst] = int(v)
    except Exception:
        pass
    return out


def _call_gemini(system, user, *, kind, max_tokens, temperature):
    api_key = _provider_api_key("gemini")
    if not api_key:
        _log.warning("LLM call (%s) requested but GEMINI_API_KEY is not configured", kind)
        return None, _UNAVAILABLE
    try:
        from google import genai
        from google.genai import types

        model = _gemini_model()
        client = genai.Client(api_key=api_key)
        t0 = _time.monotonic()
        response = client.models.generate_content(
            model=model,
            contents=f"{system}\n\n{user}",
            config=types.GenerateContentConfig(
                temperature=temperature,
                max_output_tokens=max_tokens,
            ),
        )
        duration_ms = int((_time.monotonic() - t0) * 1000)
        log_cost_event(
            "gemini", kind, model=model, duration_ms=duration_ms, **_gemini_usage_fields(response)
        )
        text = (response.text or "").strip()
        if not text:
            return None, _EMPTY
        return text, None
    except Exception as exc:
        _log.exception("Gemini call (%s) failed: %s", kind, exc)
        return None, _FAILED


# --------------------------------------------------------------------
# Claude (anthropic)
# --------------------------------------------------------------------


def _claude_text(response) -> str:
    """Concatenate text blocks from an Anthropic Messages response."""
    try:
        chunks = []
        for block in getattr(response, "content", None) or []:
            if getattr(block, "type", None) == "text":
                chunks.append(getattr(block, "text", "") or "")
        return "".join(chunks)
    except Exception:
        return ""


def _claude_usage_fields(response) -> dict:
    out: dict = {}
    try:
        usage = getattr(response, "usage", None)
        if usage is None:
            return out
        inp = getattr(usage, "input_tokens", None)
        outp = getattr(usage, "output_tokens", None)
        if inp is not None:
            out["prompt_tokens"] = int(inp)
        if outp is not None:
            out["output_tokens"] = int(outp)
        if inp is not None and outp is not None:
            out["total_tokens"] = int(inp) + int(outp)
    except Exception:
        pass
    return out


def _call_claude(system, user, *, kind, max_tokens, temperature):
    api_key = _provider_api_key("claude")
    if not api_key:
        _log.warning("LLM call (%s) requested but ANTHROPIC_API_KEY is not configured", kind)
        return None, _UNAVAILABLE
    try:
        import anthropic

        model = _claude_model()
        client = anthropic.Anthropic(api_key=api_key)
        t0 = _time.monotonic()
        response = client.messages.create(
            model=model,
            system=system,
            messages=[{"role": "user", "content": user}],
            max_tokens=max_tokens,
            temperature=temperature,
        )
        duration_ms = int((_time.monotonic() - t0) * 1000)
        log_cost_event(
            "claude", kind, model=model, duration_ms=duration_ms, **_claude_usage_fields(response)
        )
        text = _claude_text(response).strip()
        if not text:
            return None, _EMPTY
        return text, None
    except Exception as exc:
        _log.exception("Claude call (%s) failed: %s", kind, exc)
        return None, _FAILED
