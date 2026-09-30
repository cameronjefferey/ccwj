"""Share-card images for a closed position or trade.

The card is a download the browser generates on click. Nothing is stored
and nothing is public. The picture never includes an account name, broker,
balance, username, or email — privacy mode does not have to be on.

Every figure on the card (symbol, strategy, dates, realized P&L, hindsight)
is read from the viewer's own warehouse rows. The URL only carries an
identifier (which closed position or leg). Strategy, dates-as-display, and
dollars in the query string are ignored. A miss is a 404, not a card.

Hindsight, when the warehouse has an early-close counterfactual, reads:

    Sold for -$3,333 · Holding would have made +$11,933

``early_close_vs_expiry_delta`` is closing cash minus the expiry
settlement, so the hold-to-expiry result is ``realized − delta``.
"""

from __future__ import annotations

import io
import os
import re
from datetime import datetime, date

from flask import abort, request, send_file
from flask_login import current_user, login_required
from google.cloud import bigquery

from app import app
from app.bigquery_client import get_bigquery_client
from app.tenant_scope import filter_df_by_tenant_ids, tenant_sql_and

DISCLAIMER = "For learning only · not investment advice"
WORDMARK = "happytrader.me"

_FONT_DIR = os.path.join(os.path.dirname(__file__), "static", "fonts")
_SANS = os.path.join(_FONT_DIR, "InstrumentSans-SemiBold.ttf")
_MONO = os.path.join(_FONT_DIR, "JetBrainsMono-Medium.ttf")

LAYOUTS = {
    "square": (1080, 1080),
    "story": (1080, 1920),
}

# Tenant-scoped. Each frame is also passed through filter_df_by_tenant_ids,
# so the outer select must project tenant_id (see
# tests/test_tenant_filtered_queries_carry_tenant_id.py).
# Empty @trade_symbol / @session_id / @close_date means "every closed row
# for this symbol" (the position card). A specific leg sets them.
SHARE_POSITION_QUERY = """
SELECT
    symbol,
    strategy,
    status,
    realized_pnl,
    first_trade_date,
    last_trade_date,
    tenant_id
FROM `ccwj-dbt.analytics.positions_summary`
WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM(@symbol))
  {tenant_filter}
"""

SHARE_OPTION_QUERY = """
SELECT
    sc.symbol AS symbol,
    sc.strategy AS strategy,
    sc.trade_symbol AS trade_symbol,
    sc.tenant_id AS tenant_id,
    sc.open_date AS open_date,
    sc.close_date AS close_date,
    sc.total_pnl AS realized_pnl,
    oc.direction AS direction,
    eq.early_close_vs_expiry_delta AS early_close_vs_expiry_delta
FROM `ccwj-dbt.analytics.int_strategy_classification` sc
JOIN `ccwj-dbt.analytics.int_option_contracts` oc
  ON (sc.tenant_id IS NOT DISTINCT FROM oc.tenant_id)
 AND sc.account = oc.account
 AND sc.trade_symbol = oc.trade_symbol
 AND sc.user_id IS NOT DISTINCT FROM oc.user_id
LEFT JOIN `ccwj-dbt.analytics.int_option_exit_quality` eq
  ON (eq.tenant_id IS NOT DISTINCT FROM sc.tenant_id)
 AND eq.trade_symbol = sc.trade_symbol
 AND eq.user_id IS NOT DISTINCT FROM sc.user_id
WHERE sc.status = 'Closed'
  AND sc.trade_group_type = 'option_contract'
  AND UPPER(TRIM(COALESCE(sc.symbol, ''))) = UPPER(TRIM(@symbol))
  AND (@trade_symbol = '' OR sc.trade_symbol = @trade_symbol)
  {tenant_filter}
"""

SHARE_EQUITY_QUERY = """
SELECT
    symbol,
    tenant_id,
    trade_symbol,
    session_id,
    open_date,
    close_date,
    realized_pnl,
    description
FROM `ccwj-dbt.analytics.int_closed_equity_legs`
WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM(@symbol))
  AND (@session_id = '' OR CAST(session_id AS STRING) = @session_id)
  AND (
        @close_date = ''
        OR STARTS_WITH(CAST(close_date AS STRING), @close_date)
      )
  {tenant_filter}
"""

_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,15}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SESSION_RE = re.compile(r"^\d+$")


def signed_dollars(value) -> str:
    """``+$11,933`` / ``-$3,333`` / ``$0``. Whole dollars."""
    try:
        number = int(round(float(value)))
    except (TypeError, ValueError):
        return "—"
    sign = "+" if number > 0 else "-" if number < 0 else ""
    return f"{sign}${abs(number):,}"


def holding_pnl(realized_pnl, early_close_delta):
    """P&L if the contract had been held to expiry instead of closed early."""
    return float(realized_pnl) - float(early_close_delta)


def hindsight_verb(direction) -> str:
    text = str(direction or "").strip().lower()
    if text.startswith("sold") or text == "short":
        return "Closed for"
    return "Sold for"


def hindsight_line(realized_pnl, early_close_delta, direction=None):
    """The one-line comparison, or ``None`` when expiry isn't knowable."""
    if realized_pnl is None or early_close_delta is None:
        return None
    try:
        hold = holding_pnl(realized_pnl, early_close_delta)
        realized = float(realized_pnl)
    except (TypeError, ValueError):
        return None
    return (
        f"{hindsight_verb(direction)} {signed_dollars(realized)} · "
        f"Holding would have made {signed_dollars(hold)}"
    )


def _date_label(value) -> str:
    if value is None or value == "":
        return ""
    text = str(value)[:10]
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return str(value)[:40]
    return parsed.strftime("%b %-d, %Y")


def _clean(value, limit=80) -> str:
    text = " ".join(str(value or "").split())
    return text[:limit]


def build_share_card(
    *,
    symbol,
    strategy="",
    open_date=None,
    close_date=None,
    realized_pnl=None,
    early_close_delta=None,
    direction=None,
) -> dict:
    """Card payload. Identity fields are not accepted and not emitted."""
    realized = None
    if realized_pnl is not None and realized_pnl != "":
        try:
            realized = float(realized_pnl)
        except (TypeError, ValueError):
            realized = None
    delta = None
    if early_close_delta is not None and early_close_delta != "":
        try:
            delta = float(early_close_delta)
        except (TypeError, ValueError):
            delta = None
    open_label = _date_label(open_date)
    close_label = _date_label(close_date)
    if open_label and close_label and open_label != close_label:
        dates = f"{open_label}  →  {close_label}"
    else:
        dates = close_label or open_label
    return {
        "symbol": _clean(symbol, 16).upper(),
        "strategy": _clean(strategy, 48),
        "dates": dates,
        "realized_pnl": realized,
        "realized_label": signed_dollars(realized) if realized is not None else "—",
        "hindsight": hindsight_line(realized, delta, direction),
        "wordmark": WORDMARK,
        "disclaimer": DISCLAIMER,
    }


def _font(path, size):
    from PIL import ImageFont

    try:
        return ImageFont.truetype(path, size)
    except Exception:
        return ImageFont.load_default()


def _text_width(draw, text, font) -> int:
    if hasattr(draw, "textlength"):
        return int(draw.textlength(text, font=font))
    box = draw.textbbox((0, 0), text, font=font)
    return box[2] - box[0]


def render_share_png(card: dict, layout: str = "square") -> bytes:
    """PNG bytes at 1080×1080 (square) or 1080×1920 (story)."""
    from PIL import Image, ImageDraw

    if layout not in LAYOUTS:
        raise ValueError(f"unknown layout {layout}")
    width, height = LAYOUTS[layout]
    story = layout == "story"
    image = Image.new("RGB", (width, height), "#0a0e17")
    draw = ImageDraw.Draw(image)
    draw.rectangle([0, 0, width, 14], fill="#5b8cff")

    sans_sm = _font(_SANS, 36 if story else 32)
    sans_md = _font(_SANS, 44 if story else 40)
    sans_lg = _font(_SANS, 92 if story else 84)
    mono = _font(_MONO, 108 if story else 92)
    mono_sm = _font(_MONO, 36 if story else 32)

    y = 520 if story else 140
    # Wordmark: happy + trader.me
    happy = "happy"
    rest = "trader.me"
    hw = _text_width(draw, happy, sans_sm)
    rw = _text_width(draw, rest, sans_sm)
    x = (width - hw - rw) // 2
    draw.text((x, y), happy, font=sans_sm, fill="#ffffff")
    draw.text((x + hw, y), rest, font=sans_sm, fill="#5b8cff")
    y += 120 if story else 100

    symbol = card.get("symbol") or ""
    sw = _text_width(draw, symbol, sans_lg)
    draw.text(((width - sw) // 2, y), symbol, font=sans_lg, fill="#e8edf7")
    y += 130 if story else 120

    strategy = card.get("strategy") or ""
    if strategy:
        stw = _text_width(draw, strategy, sans_md)
        draw.text(((width - stw) // 2, y), strategy, font=sans_md, fill="#8a97b1")
        y += 80 if story else 70

    dates = card.get("dates") or ""
    if dates:
        dw = _text_width(draw, dates, sans_sm)
        draw.text(((width - dw) // 2, y), dates, font=sans_sm, fill="#8a97b1")
        y += 100 if story else 80

    y += 40 if story else 20
    realized = card.get("realized_label") or "—"
    try:
        pnl = float(card.get("realized_pnl"))
        color = "#28c08a" if pnl > 0 else "#f0556d" if pnl < 0 else "#e8edf7"
    except (TypeError, ValueError):
        color = "#e8edf7"
    rw = _text_width(draw, realized, mono)
    draw.text(((width - rw) // 2, y), realized, font=mono, fill=color)
    y += 150 if story else 130

    hindsight = card.get("hindsight") or ""
    if hindsight:
        # Wrap a long comparison onto two lines at the middle dot.
        parts = hindsight.split(" · ", 1)
        lines = parts if len(parts) == 2 else [hindsight]
        block_h = 36 * len(lines) + 48
        top = y
        draw.rounded_rectangle(
            [80, top, width - 80, top + block_h + 20],
            radius=24,
            fill="#121826",
        )
        line_y = top + 28
        for line in lines:
            lw = _text_width(draw, line, mono_sm)
            draw.text(((width - lw) // 2, line_y), line, font=mono_sm, fill="#e8edf7")
            line_y += 52

    disclaimer = card.get("disclaimer") or DISCLAIMER
    dw = _text_width(draw, disclaimer, sans_sm)
    draw.text(
        ((width - dw) // 2, height - (160 if story else 100)),
        disclaimer,
        font=sans_sm,
        fill="#8a97b1",
    )

    buf = io.BytesIO()
    image.save(buf, format="PNG")
    buf.seek(0)
    return buf.getvalue()


def _owned_tenant_ids(user_id):
    from app.models import get_broker_tenants_for_user

    rows = get_broker_tenants_for_user(user_id) or []
    return [str(r.get("tenant_id")) for r in rows if r.get("tenant_id")]


def _query_df(sql, params):
    client = get_bigquery_client()
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=params),
    )
    return job.to_dataframe()


def _fetch(sql_template, tenant_ids, params, col="tenant_id"):
    """Run one share lookup. Empty tenants do not become an unscoped read."""
    if not tenant_ids:
        return None
    sql = sql_template.format(tenant_filter=tenant_sql_and(tenant_ids, col=col))
    try:
        frame = _query_df(sql, params)
    except Exception as exc:
        app.logger.warning("share-card lookup failed: %s", exc)
        return None
    return filter_df_by_tenant_ids(frame, tenant_ids)


def _param(name, value):
    return bigquery.ScalarQueryParameter(name, "STRING", value if value is not None else "")


def _is_missing(value):
    if value is None:
        return True
    try:
        import pandas as pd
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return False


def _cell(row, key):
    try:
        value = row.get(key)
    except Exception:
        value = row[key] if key in getattr(row, "index", ()) else None
    if _is_missing(value):
        return None
    return value


def _as_float(value):
    if _is_missing(value) or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_date(value):
    if _is_missing(value) or value == "":
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value)[:10]
    return text if _DATE_RE.match(text) else str(value)[:40]


def _sum_realized(frame):
    if frame is None or getattr(frame, "empty", True):
        return None
    if "realized_pnl" not in frame.columns:
        return None
    total = 0.0
    seen = False
    for value in frame["realized_pnl"]:
        number = _as_float(value)
        if number is None:
            continue
        total += number
        seen = True
    return total if seen else None


def _date_bounds(frame, column):
    if frame is None or getattr(frame, "empty", True) or column not in frame.columns:
        return []
    out = []
    for value in frame[column]:
        text = _as_date(value)
        if text:
            out.append(text[:10])
    return out


def scope_tenant_ids(owned, requested):
    """Intersection of the viewer's tenants and an optional identifier list.

    ``requested is None`` means every owned tenant. A requested id the
    viewer does not own is dropped. Nothing owned → empty (fail closed).
    """
    owned_ids = []
    seen = set()
    for raw in owned or []:
        tid = str(raw or "").strip()
        if tid and tid not in seen:
            seen.add(tid)
            owned_ids.append(tid)
    if requested is None:
        return owned_ids
    picked = []
    for raw in requested:
        tid = str(raw or "").strip()
        if tid and tid in seen and tid not in picked:
            picked.append(tid)
    return picked


def _session_key(value):
    text = str(value or "").strip()
    if _SESSION_RE.match(text):
        return text
    if re.fullmatch(r"\d+\.0+", text):
        return text.split(".", 1)[0]
    return ""


def parse_share_ref(args):
    """Identifier only. Dollars, strategy, and display dates are not read."""
    kind = str(args.get("ref") or "").strip().lower()
    if kind not in ("position", "option", "equity"):
        return None
    symbol = str(args.get("symbol") or "").strip().upper()
    if not _SYMBOL_RE.match(symbol):
        return None
    spec = {"kind": kind, "symbol": symbol}
    if kind == "option":
        trade_symbol = " ".join(str(args.get("trade_symbol") or "").split())
        if not trade_symbol or len(trade_symbol) > 64:
            return None
        spec["trade_symbol"] = trade_symbol
        tenant = str(args.get("tenant") or "").strip()
        spec["tenants"] = [tenant] if tenant else None
        return spec
    if kind == "equity":
        tenant = str(args.get("tenant") or "").strip()
        session = _session_key(args.get("session"))
        close = str(args.get("close") or "").strip()[:10]
        if not tenant or not session or not _DATE_RE.match(close):
            return None
        spec["tenants"] = [tenant]
        spec["session_id"] = session
        spec["close_date"] = close
        return spec
    raw_tenants = str(args.get("tenants") or "").strip()
    if raw_tenants:
        spec["tenants"] = [part.strip() for part in raw_tenants.split(",") if part.strip()]
    else:
        spec["tenants"] = None
    return spec


def _fields_from_option_row(row):
    return {
        "symbol": _cell(row, "symbol"),
        "strategy": str(_cell(row, "strategy") or "").strip(),
        "open_date": _as_date(_cell(row, "open_date")),
        "close_date": _as_date(_cell(row, "close_date")),
        "realized_pnl": _as_float(_cell(row, "realized_pnl")),
        "early_close_delta": _as_float(_cell(row, "early_close_vs_expiry_delta")),
        "direction": _cell(row, "direction"),
    }


def lookup_share_subject(spec, owned_tenant_ids):
    """Warehouse fields for one closed position or leg, or ``None``."""
    if not spec:
        return None
    tenant_ids = scope_tenant_ids(owned_tenant_ids, spec.get("tenants"))
    if not tenant_ids:
        return None
    kind = spec["kind"]
    symbol = spec["symbol"]
    if kind == "option":
        frame = _fetch(
            SHARE_OPTION_QUERY,
            tenant_ids,
            [
                _param("symbol", symbol),
                _param("trade_symbol", spec.get("trade_symbol") or ""),
            ],
            col="sc.tenant_id",
        )
        if frame is None or len(frame) != 1:
            return None
        return _fields_from_option_row(frame.iloc[0])
    if kind == "equity":
        frame = _fetch(
            SHARE_EQUITY_QUERY,
            tenant_ids,
            [
                _param("symbol", symbol),
                _param("session_id", spec.get("session_id") or ""),
                _param("close_date", spec.get("close_date") or ""),
            ],
        )
        if frame is None or len(frame) != 1:
            return None
        row = frame.iloc[0]
        strategy = str(_cell(row, "description") or "").strip() or "Equity Sold"
        return {
            "symbol": _cell(row, "symbol") or symbol,
            "strategy": strategy,
            "open_date": _as_date(_cell(row, "open_date")),
            "close_date": _as_date(_cell(row, "close_date")),
            "realized_pnl": _as_float(_cell(row, "realized_pnl")),
            "early_close_delta": None,
            "direction": "Sold",
        }
    return _lookup_closed_position(symbol, tenant_ids)


def _lookup_closed_position(symbol, tenant_ids):
    summary = _fetch(
        SHARE_POSITION_QUERY,
        tenant_ids,
        [_param("symbol", symbol)],
    )
    if summary is None or summary.empty:
        return None
    statuses = []
    strategies = []
    for _, row in summary.iterrows():
        status = str(_cell(row, "status") or "").strip().lower()
        statuses.append(status)
        name = str(_cell(row, "strategy") or "").strip()
        if name and name not in strategies:
            strategies.append(name)
    if not statuses or any(status != "closed" for status in statuses):
        return None
    options = _fetch(
        SHARE_OPTION_QUERY,
        tenant_ids,
        [_param("symbol", symbol), _param("trade_symbol", "")],
        col="sc.tenant_id",
    )
    equity = _fetch(
        SHARE_EQUITY_QUERY,
        tenant_ids,
        [
            _param("symbol", symbol),
            _param("session_id", ""),
            _param("close_date", ""),
        ],
    )
    leg_realized = None
    opens = []
    closes = []
    if options is not None and not options.empty:
        leg_realized = _sum_realized(options)
        opens.extend(_date_bounds(options, "open_date"))
        closes.extend(_date_bounds(options, "close_date"))
    if equity is not None and not equity.empty:
        eq_realized = _sum_realized(equity)
        if eq_realized is not None:
            leg_realized = (leg_realized or 0.0) + eq_realized
        opens.extend(_date_bounds(equity, "open_date"))
        closes.extend(_date_bounds(equity, "close_date"))
    if leg_realized is None:
        leg_realized = _sum_realized(summary)
    if not opens:
        opens = _date_bounds(summary, "first_trade_date")
    if not closes:
        closes = _date_bounds(summary, "last_trade_date")
    delta = None
    direction = None
    n_opt = 0 if options is None else len(options)
    n_eq = 0 if equity is None else len(equity)
    if n_opt == 1 and n_eq == 0:
        only = options.iloc[0]
        delta = _as_float(_cell(only, "early_close_vs_expiry_delta"))
        direction = _cell(only, "direction")
    strategy = strategies[0] if len(strategies) == 1 else "Closed position"
    return {
        "symbol": _cell(summary.iloc[0], "symbol") or symbol,
        "strategy": strategy,
        "open_date": min(opens) if opens else None,
        "close_date": max(closes) if closes else None,
        "realized_pnl": leg_realized,
        "early_close_delta": delta,
        "direction": direction,
    }


def card_for_viewer(args, owned_tenant_ids):
    """Card dict from warehouse data, or ``None`` when the leg isn't theirs."""
    spec = parse_share_ref(args)
    if spec is None:
        return None
    fields = lookup_share_subject(spec, owned_tenant_ids)
    if not fields:
        return None
    return build_share_card(**fields)


@app.route("/share/card.png")
@login_required
def share_card_png():
    """Authenticated, unstored PNG. Numbers come from the viewer's warehouse."""
    layout = (request.args.get("layout") or "square").strip().lower()
    if layout not in LAYOUTS:
        abort(400)
    card = card_for_viewer(request.args, _owned_tenant_ids(current_user.id))
    if card is None:
        abort(404)
    png = render_share_png(card, layout)
    filename = f"{card['symbol'] or 'trade'}-{layout}.png"
    return send_file(
        io.BytesIO(png),
        mimetype="image/png",
        as_attachment=False,
        download_name=filename,
        max_age=0,
    )
