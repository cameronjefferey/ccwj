"""Provider-agnostic LLM narration layer.

Every AI surface in the app (/insights, /strategy-fit) follows the same
two-layer pattern: a DETERMINISTIC brief is built from tenant-scoped
BigQuery data (no LLM, no hallucination risk), then a model NARRATES that
brief into prose. This module owns the narration call so the vendor choice
(Gemini vs Claude) and the specific model live in exactly one place.

The model never touches BigQuery or tenant scoping — it only ever sees the
pre-built text brief the caller hands it. Swapping providers/models has
zero bearing on the multi-tenant isolation guarantees enforced upstream.

Model selection
---------------
Models are described in MODEL_CATALOG (key -> provider + API model id +
display label + cost tier). Which of those are *offerable* in the product
is gated two ways:

1. Allowlist (env `SELECTABLE_LLM_MODELS`, comma-separated catalog keys).
   Defaults to the low-cost set so paid models can't be picked until the
   operator explicitly opts in. This is how "add a pricier model later"
   works — append its key to the env var.
2. Provider key present — a model is only really selectable when its
   vendor key (`GEMINI_API_KEY` / `ANTHROPIC_API_KEY`) is configured.

`call_llm(..., model_key=...)` takes the per-user choice; anything missing
or not currently selectable falls back to `default_model_key()` (which
honors the legacy `LLM_PROVIDER` env as a tie-breaker).

Both vendor SDKs are imported lazily inside their branch so the unused one
is never a hard dependency at import time.
"""
from __future__ import annotations

import logging
import os
import time as _time

from app.cost_tracking import log_cost_event

_log = logging.getLogger("happytrader.llm")

# key -> metadata. `model` is the exact API model id; `provider` selects the
# SDK branch; `tier` is informational (drives the "free"-only default
# allowlist and the UI label). Add new models here, then expose them by
# adding their key to SELECTABLE_LLM_MODELS.
MODEL_CATALOG = {
    "gemini-2.5-flash": {
        "label": "Gemini 2.5 Flash",
        "provider": "gemini",
        "model": "gemini-2.5-flash",
        "tier": "free",
    },
    "claude-haiku-4-5": {
        "label": "Claude Haiku 4.5",
        "provider": "claude",
        "model": "claude-haiku-4-5",
        "tier": "low-cost",
    },
    # --- Paid tiers: present in the catalog but NOT in the default
    # allowlist. Add the key to SELECTABLE_LLM_MODELS to offer them. ---
    "claude-sonnet-4-6": {
        "label": "Claude Sonnet 4.6",
        "provider": "claude",
        "model": "claude-sonnet-4-6",
        "tier": "paid",
    },
    "claude-opus-4-8": {
        "label": "Claude Opus 4.8",
        "provider": "claude",
        "model": "claude-opus-4-8",
        "tier": "paid",
    },
}

# Offered by default until the operator opts paid models in via env.
_DEFAULT_ALLOWLIST = ["gemini-2.5-flash", "claude-haiku-4-5"]

_UNAVAILABLE = "AI is temporarily unavailable. Try again in a few minutes."
_FAILED = "Couldn't generate that right now. Try again in a moment."
_EMPTY = "The model returned an empty response. Try again in a moment."


def active_provider() -> str:
    """Legacy env default provider ('gemini' or 'claude').

    Used only as a tie-breaker for default_model_key(); per-request model
    choice flows through call_llm(model_key=...).
    """
    return (os.environ.get("LLM_PROVIDER", "gemini") or "gemini").strip().lower()


def _allowlist_keys() -> list[str]:
    raw = (os.environ.get("SELECTABLE_LLM_MODELS", "") or "").strip()
    if not raw:
        return list(_DEFAULT_ALLOWLIST)
    keys = [k.strip() for k in raw.split(",") if k.strip()]
    # Ignore unknown keys so a typo can't 500 the picker.
    return [k for k in keys if k in MODEL_CATALOG]


def _provider_has_key(provider: str) -> bool:
    if provider == "claude":
        return bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())
    return bool(os.environ.get("GEMINI_API_KEY", "").strip())


def selectable_models() -> list[dict]:
    """Models offerable right now: in the allowlist AND vendor key present.

    Returns a list of {key, label, provider, tier} preserving allowlist
    order. This is what the UI dropdown renders.
    """
    out = []
    for key in _allowlist_keys():
        spec = MODEL_CATALOG[key]
        if _provider_has_key(spec["provider"]):
            out.append({
                "key": key,
                "label": spec["label"],
                "provider": spec["provider"],
                "tier": spec["tier"],
            })
    return out


def selectable_model_keys() -> set[str]:
    return {m["key"] for m in selectable_models()}


def default_model_key() -> str | None:
    """The model to use when the user hasn't chosen (or chose one that's no
    longer offerable). Prefers a model matching the legacy LLM_PROVIDER env
    so existing deployments keep their current behavior; otherwise the first
    selectable model. Returns None when nothing is selectable."""
    models = selectable_models()
    if not models:
        return None
    pref_provider = active_provider()
    for m in models:
        if m["provider"] == pref_provider:
            return m["key"]
    return models[0]["key"]


def resolve_model_key(model_key: str | None) -> str | None:
    """Validate a requested key against what's currently selectable, falling
    back to the default. Never trusts an arbitrary string into the catalog."""
    if model_key and model_key in selectable_model_keys():
        return model_key
    return default_model_key()


def model_label(model_key: str | None) -> str:
    spec = MODEL_CATALOG.get(model_key or "")
    return spec["label"] if spec else ""


def llm_available() -> bool:
    """True when at least one model is selectable (allowlisted + keyed)."""
    return bool(selectable_models())


def call_llm(system: str, user: str, *, kind: str, max_tokens: int,
             temperature: float, model_key: str | None = None):
    """Run one narration turn against the resolved model.

    system      : instructions / role (Claude's top-level system field;
                  prepended to the prompt for Gemini which has no separate slot)
    user        : the deterministic data brief to narrate
    kind        : cost-event tag, e.g. 'coach.generate' / 'coach.ask'
    model_key   : the user's chosen catalog key (validated; falls back to
                  default_model_key() when missing or not selectable)

    Returns (text, None) on success or (None, user_facing_error). Cost is
    logged here (vendor + kind + model + duration_ms + token counts) so
    callers never repeat that bookkeeping.
    """
    key = resolve_model_key(model_key)
    if not key:
        _log.warning("LLM call (%s) requested but no model is selectable", kind)
        return None, _UNAVAILABLE
    spec = MODEL_CATALOG[key]
    if spec["provider"] == "claude":
        return _call_claude(
            spec["model"], system, user, kind=kind, max_tokens=max_tokens, temperature=temperature
        )
    return _call_gemini(
        spec["model"], system, user, kind=kind, max_tokens=max_tokens, temperature=temperature
    )


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


def _call_gemini(model, system, user, *, kind, max_tokens, temperature):
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        _log.warning("LLM call (%s) requested but GEMINI_API_KEY is not configured", kind)
        return None, _UNAVAILABLE
    try:
        from google import genai
        from google.genai import types

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


def _call_claude(model, system, user, *, kind, max_tokens, temperature):
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        _log.warning("LLM call (%s) requested but ANTHROPIC_API_KEY is not configured", kind)
        return None, _UNAVAILABLE
    try:
        import anthropic

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
