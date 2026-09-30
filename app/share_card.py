"""Share-card images for a closed position or trade.

The card is a download the browser generates on click. Nothing is stored
and nothing is public. The picture never includes an account name, broker,
balance, username, or email — privacy mode does not have to be on.

Hindsight, when the warehouse has an early-close counterfactual, reads:

    Sold for -$3,333 · Holding would have made +$11,933

``early_close_vs_expiry_delta`` is closing cash minus the expiry
settlement, so the hold-to-expiry result is ``realized − delta``.
"""

from __future__ import annotations

import io
import os
from datetime import datetime

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

# Tenant-scoped. The frame is also passed through filter_df_by_tenant_ids,
# so the outer select must project tenant_id (see
# tests/test_tenant_filtered_queries_carry_tenant_id.py).
SHARE_EXIT_QUERY = """
SELECT
    realized_pnl,
    early_close_vs_expiry_delta,
    direction,
    open_date,
    close_date,
    symbol,
    trade_symbol,
    tenant_id
FROM `ccwj-dbt.analytics.int_option_exit_quality`
WHERE UPPER(symbol) = UPPER(@symbol)
  AND trade_symbol = @trade_symbol
  {tenant_filter}
LIMIT 8
"""


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


def lookup_exit_quality(symbol, trade_symbol, tenant_ids):
    """Warehouse hindsight for this viewer's contract, or ``None``.

    Fail closed: an empty tenant list does not run an unscoped query.
    """
    symbol = _clean(symbol, 16)
    trade_symbol = _clean(trade_symbol, 64)
    if not symbol or not trade_symbol:
        return None
    if not tenant_ids:
        return None
    sql = SHARE_EXIT_QUERY.format(tenant_filter=tenant_sql_and(tenant_ids))
    try:
        client = get_bigquery_client()
        job = client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("symbol", "STRING", symbol),
                bigquery.ScalarQueryParameter("trade_symbol", "STRING", trade_symbol),
            ]),
        )
        frame = job.to_dataframe()
    except Exception as exc:
        app.logger.warning("share-card exit lookup failed: %s", exc)
        return None
    frame = filter_df_by_tenant_ids(frame, tenant_ids)
    if frame is None or frame.empty:
        return None
    row = frame.iloc[0]
    return {
        "realized_pnl": row.get("realized_pnl"),
        "early_close_delta": row.get("early_close_vs_expiry_delta"),
        "direction": row.get("direction"),
        "open_date": row.get("open_date"),
        "close_date": row.get("close_date"),
    }


def _float_arg(name):
    raw = request.args.get(name, "")
    if raw == "":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


@app.route("/share/card.png")
@login_required
def share_card_png():
    """Authenticated, unstored PNG. Query string is the trade the page already showed."""
    layout = (request.args.get("layout") or "square").strip().lower()
    if layout not in LAYOUTS:
        abort(400)
    symbol = _clean(request.args.get("symbol"), 16)
    if not symbol:
        abort(400)
    strategy = _clean(request.args.get("strategy"), 48)
    open_date = _clean(request.args.get("open"), 32)
    close_date = _clean(request.args.get("close"), 32)
    realized = _float_arg("realized")
    direction = _clean(request.args.get("direction"), 24)
    delta = None
    trade_symbol = _clean(request.args.get("trade_symbol"), 64)
    if trade_symbol and getattr(current_user, "is_authenticated", False):
        found = lookup_exit_quality(
            symbol, trade_symbol, _owned_tenant_ids(current_user.id),
        )
        if found:
            if found.get("realized_pnl") is not None:
                try:
                    realized = float(found["realized_pnl"])
                except (TypeError, ValueError):
                    pass
            delta = found.get("early_close_delta")
            direction = found.get("direction") or direction
            if found.get("open_date"):
                open_date = str(found["open_date"])[:10]
            if found.get("close_date"):
                close_date = str(found["close_date"])[:10]
    card = build_share_card(
        symbol=symbol,
        strategy=strategy,
        open_date=open_date,
        close_date=close_date,
        realized_pnl=realized,
        early_close_delta=delta,
        direction=direction,
    )
    png = render_share_png(card, layout)
    filename = f"{card['symbol'] or 'trade'}-{layout}.png"
    return send_file(
        io.BytesIO(png),
        mimetype="image/png",
        as_attachment=False,
        download_name=filename,
        max_age=0,
    )
