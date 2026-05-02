"""
Cost-tracking log lines.

We do not (yet) push metrics to a vendor — that's overkill for closed
beta. Instead every call to a metered API (Gemini, BigQuery) emits a
single tagged log line that the operator can grep out of Render to
estimate daily spend without leaving the existing logging stack.

Format:
    COST_EVENT vendor=<name> kind=<short-tag> user=<id|anon>
        <key1>=<value1> <key2>=<value2> ...

Keep the values numeric where possible (token counts, byte counts,
millisecond duration) so a future log-aggregation step can sum them.
The function is wrapped in a try/except so a logging failure never
breaks the request flow — cost telemetry is observability, not the
critical path.
"""
from __future__ import annotations

import logging

from flask_login import current_user


_log = logging.getLogger("happytrader.cost")


def _user_tag() -> str:
    try:
        if current_user.is_authenticated:
            return f"u{current_user.id}"
    except Exception:
        pass
    return "anon"


def log_cost_event(vendor: str, kind: str, **fields) -> None:
    """Emit a single COST_EVENT line.

    vendor: 'gemini', 'bigquery', etc.
    kind:   short tag for the call type ('coach.generate', 'coach.ask',
            'strategy_fit.generate', 'positions_summary', ...). Used for
            grouping in grep / log queries.
    fields: any extra key=value pairs (token counts, bytes processed,
            duration_ms). Values are coerced via repr() so unusual types
            don't crash the logger.
    """
    try:
        parts = [
            f"vendor={vendor}",
            f"kind={kind}",
            f"user={_user_tag()}",
        ]
        for k, v in fields.items():
            if v is None:
                continue
            parts.append(f"{k}={v}")
        _log.info("COST_EVENT " + " ".join(parts))
    except Exception:  # pragma: no cover (defensive)
        pass
