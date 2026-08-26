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

1. Allowlist (env `SELECTABLE_LLM_MODELS`, comma-separated catalog keys)
   for included (free / low-cost) models. Defaults to Flash + Haiku.
2. Provider key present — a model is only really selectable when its
   vendor key (`GEMINI_API_KEY` / `ANTHROPIC_API_KEY`) is configured.

Paid-tier catalog rows appear in the picker automatically whenever the
vendor key is set. The spend gate is the HappyTrader AI add-on
(`allow_paid=True` from `user_can_use_paid_llm`), not the env allowlist.

`call_llm(..., model_key=...)` takes the per-user choice; anything missing
or not currently selectable falls back to `default_model_key()` (which
honors the legacy `LLM_PROVIDER` env as a tie-breaker). Paid-tier keys
without `allow_paid` fall back to a free/low-cost default.

Both vendor SDKs are imported lazily inside their branch so the unused one
is never a hard dependency at import time.
"""
from __future__ import annotations

import logging
import os
import time as _time

from app.cost_tracking import log_cost_event

_log = logging.getLogger("happytrader.llm")

# How to add a model to the /insights dropdown
# -------------------------------------------
# 1. Add a row here (key, display label, provider, exact API model id, tier).
# 2. tier="paid"  → HappyTrader AI add-on group. Shown whenever the vendor
#    key is set; spend is gated by users.ai_* / STRIPE_PRICE_AI_MONTHLY.
# 3. tier="free" or "low-cost" → Included group. Also add the key to
#    _DEFAULT_ALLOWLIST (or SELECTABLE_LLM_MODELS) so it appears without
#    an env change.
# A missing GEMINI_API_KEY / ANTHROPIC_API_KEY hides that vendor's rows.
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
    "gemini-2.5-pro": {
        "label": "Gemini 2.5 Pro",
        "provider": "gemini",
        "model": "gemini-2.5-pro",
        "tier": "paid",
    },
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


def model_is_paid(model_key: str | None) -> bool:
    spec = MODEL_CATALOG.get(model_key or "")
    return bool(spec and spec.get("tier") == "paid")


def _picker_row(key: str, spec: dict) -> dict:
    paid = spec.get("tier") == "paid"
    return {
        "key": key,
        "label": spec["label"],
        "provider": spec["provider"],
        "tier": spec["tier"],
        "group": "addon" if paid else "included",
    }


def selectable_models() -> list[dict]:
    """Models offerable right now: allowlisted + vendor key, plus paid
    catalog rows whenever the vendor key is present.

    Paid models are shown in the picker even without SELECTABLE_LLM_MODELS
    — the AI add-on (not the env allowlist) is the spend gate. Returns
    {key, label, provider, tier, group} with group ``included`` or ``addon``.
    """
    out = []
    seen = set()
    for key in _allowlist_keys():
        spec = MODEL_CATALOG[key]
        if _provider_has_key(spec["provider"]):
            out.append(_picker_row(key, spec))
            seen.add(key)
    for key, spec in MODEL_CATALOG.items():
        if key in seen or spec.get("tier") != "paid":
            continue
        if _provider_has_key(spec["provider"]):
            out.append(_picker_row(key, spec))
    return out


def selectable_model_keys() -> set[str]:
    return {m["key"] for m in selectable_models()}


def default_model_key() -> str | None:
    """The model to use when the user hasn't chosen (or chose one that's no
    longer offerable). Prefers a non-paid model matching the legacy
    LLM_PROVIDER env so existing deployments keep their current behavior;
    otherwise the first selectable unpaid model, then any selectable.
    Returns None when nothing is selectable."""
    models = selectable_models()
    if not models:
        return None
    unpaid = [m for m in models if m["tier"] != "paid"]
    pool = unpaid or models
    pref_provider = active_provider()
    for m in pool:
        if m["provider"] == pref_provider:
            return m["key"]
    return pool[0]["key"]


def resolved_user_model_key(user_id, saved_key: str | None) -> str | None:
    """Resolve the user's saved catalog key against the live allowlist and
    the AI add-on. Callers should use this instead of resolve_model_key
    plus a hand-rolled paid check."""
    from app.llm_access import user_can_use_paid_llm
    return resolve_model_key(
        saved_key, allow_paid=user_can_use_paid_llm(user_id),
    )


def resolve_model_key(model_key: str | None, *, allow_paid: bool = False) -> str | None:
    """Validate a requested key against what's currently selectable, falling
    back to the default. Never trusts an arbitrary string into the catalog.
    Paid keys without ``allow_paid`` fall back so a crafted POST cannot
    burn Opus on a trial account."""
    if model_key and model_key in selectable_model_keys():
        if model_is_paid(model_key) and not allow_paid:
            return default_model_key()
        return model_key
    return default_model_key()


def model_label(model_key: str | None) -> str:
    spec = MODEL_CATALOG.get(model_key or "")
    return spec["label"] if spec else ""


def llm_available() -> bool:
    """True when at least one model is selectable (allowlisted + keyed)."""
    return bool(selectable_models())


def _normalized_history(history):
    """Keep only well-formed user/assistant turns for a multi-turn call."""
    out = []
    for turn in history or []:
        role = (turn.get("role") or "").strip()
        content = (turn.get("content") or "").strip()
        if role in ("user", "assistant") and content:
            out.append({"role": role, "content": content})
    return out


def call_llm(system: str, user: str, *, kind: str, max_tokens: int,
             temperature: float, model_key: str | None = None,
             allow_paid: bool = False, history=None):
    """Run one narration turn against the resolved model.

    system      : instructions / role (Claude's top-level system field;
                  prepended to the prompt for Gemini which has no separate slot)
    user        : the latest user turn (brief for generate; the question for Ask)
    kind        : cost-event tag, e.g. 'coach.generate' / 'coach.ask'
    model_key   : the user's chosen catalog key (validated; falls back to
                  default_model_key() when missing or not selectable)
    allow_paid  : caller has already checked the AI add-on
    history     : prior [{role, content}] turns, oldest first (Ask AI thread)

    Returns (text, None) on success or (None, user_facing_error). Cost is
    logged here (vendor + kind + model + duration_ms + token counts) so
    callers never repeat that bookkeeping.
    """
    key = resolve_model_key(model_key, allow_paid=allow_paid)
    if not key:
        _log.warning("LLM call (%s) requested but no model is selectable", kind)
        return None, _UNAVAILABLE
    spec = MODEL_CATALOG[key]
    prior = _normalized_history(history)
    if spec["provider"] == "claude":
        return _call_claude(
            spec["model"], system, user, kind=kind, max_tokens=max_tokens,
            temperature=temperature, history=prior,
        )
    return _call_gemini(
        spec["model"], system, user, kind=kind, max_tokens=max_tokens,
        temperature=temperature, history=prior,
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


def _call_gemini(model, system, user, *, kind, max_tokens, temperature,
                 history=None):
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        _log.warning("LLM call (%s) requested but GEMINI_API_KEY is not configured", kind)
        return None, _UNAVAILABLE
    try:
        from google import genai
        from google.genai import types

        parts = [system]
        for turn in history or []:
            label = "User" if turn["role"] == "user" else "Assistant"
            parts.append(f"{label}: {turn['content']}")
        parts.append(f"User: {user}")
        client = genai.Client(api_key=api_key)
        t0 = _time.monotonic()
        response = client.models.generate_content(
            model=model,
            contents="\n\n".join(parts),
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


def _call_claude(model, system, user, *, kind, max_tokens, temperature,
                 history=None):
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        _log.warning("LLM call (%s) requested but ANTHROPIC_API_KEY is not configured", kind)
        return None, _UNAVAILABLE
    try:
        import anthropic

        messages = list(history or [])
        messages.append({"role": "user", "content": user})
        client = anthropic.Anthropic(api_key=api_key)
        t0 = _time.monotonic()
        response = client.messages.create(
            model=model,
            system=system,
            messages=messages,
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
