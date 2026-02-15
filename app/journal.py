"""Trade Journal routes — behavioral logging and correlation."""
import json
from datetime import datetime
from flask import render_template, request, redirect, url_for, flash, Response
from flask_login import login_required, current_user
from app import app
from app.models import (
    get_accounts_for_user,
    get_journal_entry,
    list_journal_entries,
    create_journal_entry,
    update_journal_entry,
    delete_journal_entry,
    JOURNAL_TAG_OPTIONS,
    JOURNAL_MOOD_OPTIONS,
)


@app.route("/journal")
@login_required
def journal():
    """List journal entries with filters."""
    symbol = request.args.get("symbol", "").strip() or None
    strategy = request.args.get("strategy", "").strip() or None
    tag = request.args.get("tag", "").strip().lower() or None

    entries = list_journal_entries(
        current_user.id,
        symbol=symbol,
        strategy=strategy,
        tag=tag,
    )

    return render_template(
        "journal.html",
        title="Trade Journal",
        entries=entries,
        symbol_filter=symbol or "",
        strategy_filter=strategy or "",
        tag_filter=tag or "",
        tag_options=JOURNAL_TAG_OPTIONS,
        mood_options=JOURNAL_MOOD_OPTIONS,
    )


@app.route("/journal/new", methods=["GET", "POST"])
@login_required
def journal_new():
    """Create a new journal entry."""
    accounts = get_accounts_for_user(current_user.id)
    if not accounts:
        flash("Link an account first. Upload your Schwab data to get started.", "warning")
        return redirect(url_for("upload"))

    if request.method == "POST":
        account = request.form.get("account", "").strip()
        symbol = request.form.get("symbol", "").strip().upper()
        strategy = request.form.get("strategy", "").strip()
        trade_open_date = request.form.get("trade_open_date", "").strip()

        if not all([account, symbol, strategy, trade_open_date]):
            flash("Account, Symbol, Strategy, and Open Date are required.", "danger")
            return redirect(request.url)

        confidence = request.form.get("confidence")
        confidence = int(confidence) if confidence else None
        sleep_quality = request.form.get("sleep_quality")
        sleep_quality = int(sleep_quality) if sleep_quality else None

        tags = request.form.getlist("tags")

        create_journal_entry(
            current_user.id,
            account=account,
            symbol=symbol,
            strategy=strategy,
            trade_open_date=trade_open_date,
            trade_close_date=request.form.get("trade_close_date") or None,
            trade_symbol=request.form.get("trade_symbol") or None,
            thesis=request.form.get("thesis") or None,
            notes=request.form.get("notes") or None,
            reflection=request.form.get("reflection") or None,
            confidence=confidence,
            mood=request.form.get("mood") or None,
            sleep_quality=sleep_quality,
            entry_time=request.form.get("entry_time") or None,
            tags=tags,
        )
        flash("Journal entry created.", "success")
        return redirect(url_for("journal"))

    # Pre-fill from query params (e.g. from position detail)
    account = request.args.get("account", accounts[0] if accounts else "")
    symbol = request.args.get("symbol", "")
    strategy = request.args.get("strategy", "")

    return render_template(
        "journal_form.html",
        title="New Journal Entry",
        entry=None,
        accounts=accounts,
        tag_options=JOURNAL_TAG_OPTIONS,
        mood_options=JOURNAL_MOOD_OPTIONS,
        prefill={"account": account, "symbol": symbol, "strategy": strategy},
    )


@app.route("/journal/<int:entry_id>", methods=["GET", "POST"])
@login_required
def journal_edit(entry_id):
    """View or edit a journal entry."""
    entry = get_journal_entry(entry_id, current_user.id)
    if not entry:
        flash("Journal entry not found.", "danger")
        return redirect(url_for("journal"))

    if request.method == "POST":
        confidence = request.form.get("confidence")
        confidence = int(confidence) if confidence else None
        sleep_quality = request.form.get("sleep_quality")
        sleep_quality = int(sleep_quality) if sleep_quality else None

        update_journal_entry(
            entry_id,
            current_user.id,
            trade_close_date=request.form.get("trade_close_date") or None,
            trade_symbol=request.form.get("trade_symbol") or None,
            thesis=request.form.get("thesis") or None,
            notes=request.form.get("notes") or None,
            reflection=request.form.get("reflection") or None,
            confidence=confidence,
            mood=request.form.get("mood") or None,
            sleep_quality=sleep_quality,
            entry_time=request.form.get("entry_time") or None,
            tags=request.form.getlist("tags"),
        )
        flash("Journal entry updated.", "success")
        return redirect(url_for("journal"))

    return render_template(
        "journal_form.html",
        title="Edit Journal Entry",
        entry=entry,
        accounts=[entry["account"]],
        tag_options=JOURNAL_TAG_OPTIONS,
        mood_options=JOURNAL_MOOD_OPTIONS,
        prefill=None,
    )


@app.route("/journal/<int:entry_id>/delete", methods=["POST"])
@login_required
def journal_delete(entry_id):
    """Delete a journal entry."""
    if delete_journal_entry(entry_id, current_user.id):
        flash("Journal entry deleted.", "success")
    else:
        flash("Journal entry not found.", "danger")
    return redirect(url_for("journal"))


@app.route("/journal/export")
@login_required
def journal_export():
    """Export journal data as JSON. Your data, your control—never lose your 2-year dataset."""
    entries = list_journal_entries(current_user.id, limit=10000)
    export = {
        "version": 1,
        "exported_at": datetime.utcnow().isoformat() + "Z",
        "entries": [
            {
                "account": e["account"],
                "symbol": e["symbol"],
                "strategy": e["strategy"],
                "trade_open_date": e["trade_open_date"],
                "trade_close_date": e.get("trade_close_date"),
                "trade_symbol": e.get("trade_symbol"),
                "thesis": e.get("thesis") or "",
                "notes": e.get("notes") or "",
                "reflection": e.get("reflection") or "",
                "confidence": e.get("confidence"),
                "mood": e.get("mood"),
                "sleep_quality": e.get("sleep_quality"),
                "entry_time": e.get("entry_time"),
                "tags": e.get("tags", []),
                "created_at": e.get("created_at"),
                "updated_at": e.get("updated_at"),
            }
            for e in entries
        ],
    }
    filename = f"happytrader_journal_{datetime.utcnow().strftime('%Y%m%d')}.json"
    return Response(
        json.dumps(export, indent=2),
        mimetype="application/json",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/journal/import", methods=["GET", "POST"])
@login_required
def journal_import():
    """Import journal entries from a previously exported JSON file."""
    if request.method == "POST":
        f = request.files.get("file")
        if not f or not f.filename:
            flash("No file selected.", "danger")
            return redirect(url_for("journal_import"))

        try:
            data = json.load(f)
            entries = data.get("entries", [])
            if not isinstance(entries, list):
                raise ValueError("Invalid format")
        except (json.JSONDecodeError, ValueError) as e:
            flash(f"Invalid file: {e}", "danger")
            return redirect(url_for("journal_import"))

        accounts = get_accounts_for_user(current_user.id)
        imported = 0
        for e in entries:
            account = e.get("account", "")
            symbol = (e.get("symbol") or "").upper()
            strategy = e.get("strategy", "")
            trade_open_date = e.get("trade_open_date", "")
            if not all([account, symbol, strategy, trade_open_date]):
                continue
            if accounts and account not in accounts:
                continue
            create_journal_entry(
                current_user.id,
                account=account,
                symbol=symbol,
                strategy=strategy,
                trade_open_date=trade_open_date,
                trade_close_date=e.get("trade_close_date"),
                trade_symbol=e.get("trade_symbol"),
                thesis=e.get("thesis"),
                notes=e.get("notes"),
                reflection=e.get("reflection"),
                confidence=e.get("confidence"),
                mood=e.get("mood"),
                sleep_quality=e.get("sleep_quality"),
                entry_time=e.get("entry_time"),
                tags=e.get("tags") or [],
            )
            imported += 1

        flash(f"Imported {imported} journal entries.", "success")
        return redirect(url_for("journal"))

    return render_template("journal_import.html", title="Import Journal")
