"""Read-only profile of CHECK 12 (coach closed counts vs classified contracts).

Prints counts and category shares only. Account ids, tickers, prices, and
dollar amounts never leave BigQuery. Every statement is a SELECT.
"""
from __future__ import annotations

import re
import sys

PROJECT = "ccwj-dbt"
DS = f"`{PROJECT}.analytics`"
RAW = f"`{PROJECT}.analytics_raw`"

# A printed line is key=value. Values are counts, percents, or short tokens.
_LINE = re.compile(r"^[a-z][a-z0-9_]*=[A-Za-z0-9_%.-]+$")
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|create|drop|alter|truncate|grant|revoke)\b",
    re.IGNORECASE,
)


def _canon(expiry, strike, cp, und, sym):
    """Same compact OCC id as canonical_option_trade_symbol, inlined."""
    return f"""
    case
        when {expiry} is not null
         and {strike} is not null
         and upper(trim(cast({cp} as string))) in ('C', 'P')
         and regexp_contains(
                upper(trim(cast({und} as string))),
                r'^[A-Z][A-Z0-9.-]{{0,5}}$'
             )
        then concat(
            upper(trim(cast({und} as string))),
            format_date('%y%m%d', {expiry}),
            upper(trim(cast({cp} as string))),
            lpad(
                cast(cast(round({strike} * 1000) as int64) as string),
                8,
                '0'
            )
        )
        else upper(trim(cast({sym} as string)))
    end
    """


def _raw_canon(sym):
    """Compact id parsed from a raw Symbol cell. Equities fall back to the text."""
    s = f"upper(trim(cast({sym} as string)))"
    osi = f"regexp_extract({s}, r'(\\d{{6}}[CP]\\d{{8}})')"
    expiry = f"""
        coalesce(
            safe.parse_date('%m/%d/%Y', nullif(split({s}, ' ')[safe_offset(1)], '')),
            case
                when {osi} is not null
                then date(
                    2000 + cast(substr({osi}, 1, 2) as int64),
                    cast(substr({osi}, 3, 2) as int64),
                    cast(substr({osi}, 5, 2) as int64)
                )
            end
        )
    """
    strike = f"""
        coalesce(
            safe_cast(split({s}, ' ')[safe_offset(2)] as float64),
            safe_cast(safe_divide(safe_cast(substr({osi}, 8, 8) as int64), 1000) as float64)
        )
    """
    cp = f"""
        coalesce(
            case
                when nullif(split({s}, ' ')[safe_offset(3)], '') in ('C', 'P')
                then nullif(split({s}, ' ')[safe_offset(3)], '')
            end,
            substr({osi}, 7, 1)
        )
    """
    und = f"""
        case
            when regexp_contains(replace({s}, ' ', ''), r'^[A-Z][A-Z0-9.-]{{0,5}}\\d{{6}}[CP]\\d{{8}}$')
            then regexp_extract(replace({s}, ' ', ''), r'^([A-Z][A-Z0-9.-]{{0,5}})\\d{{6}}[CP]\\d{{8}}$')
            else trim(split({s}, ' ')[safe_offset(0)])
        end
    """
    return _canon(expiry, strike, cp, und, s)


_EXTRAS = f"""
extras as (
    select
        oc.tenant_id,
        oc.account,
        oc.user_id,
        oc.trade_symbol,
        oc.underlying_symbol,
        oc.option_expiry,
        oc.option_strike,
        oc.option_type,
        oc.direction,
        oc.close_type,
        oc.close_date,
        oc.status,
        coalesce(oc.opened_before_history, false) as obh,
        {_canon(
            'oc.option_expiry',
            'oc.option_strike',
            'oc.option_type',
            'oc.underlying_symbol',
            'oc.trade_symbol',
        )} as canon
    from {DS}.int_option_contracts oc
    left join {DS}.int_strategy_classification sc
        on oc.account = sc.account
        and oc.user_id is not distinct from sc.user_id
        and oc.tenant_id is not distinct from sc.tenant_id
        and oc.trade_symbol = sc.trade_symbol
        and sc.trade_group_type = 'option_contract'
    where oc.status = 'Closed'
      and oc.close_date is not null
      and sc.trade_symbol is null
)
"""


def queries():
    """Named SELECT statements. The outer select list is counts only."""
    canon_h = _canon(
        "h.option_expiry",
        "h.option_strike",
        "h.option_type",
        "h.underlying_symbol",
        "h.trade_symbol",
    )
    raw_canon = _raw_canon("r.Symbol")
    return [
        (
            "tables",
            f"""
            select
                max(if(table_id = 'mart_coaching_signals',
                       timestamp_diff(current_timestamp(), timestamp_millis(last_modified_time), hour),
                       null)) as coach_age_hours,
                max(if(table_id = 'int_strategy_classification',
                       timestamp_diff(current_timestamp(), timestamp_millis(last_modified_time), hour),
                       null)) as class_age_hours,
                max(if(table_id = 'int_option_contracts',
                       timestamp_diff(current_timestamp(), timestamp_millis(last_modified_time), hour),
                       null)) as contracts_age_hours,
                max(if(table_id = 'stg_history',
                       timestamp_diff(current_timestamp(), timestamp_millis(last_modified_time), hour),
                       null)) as history_age_hours,
                max(if(table_id = 'int_option_exit_analysis',
                       timestamp_diff(current_timestamp(), timestamp_millis(last_modified_time), hour),
                       null)) as exit_age_hours
            from {DS}.__TABLES__
            where table_id in (
                'mart_coaching_signals',
                'int_strategy_classification',
                'int_option_contracts',
                'stg_history',
                'int_option_exit_analysis'
            )
            """,
        ),
        (
            "spellings",
            f"""
            select
                count(*) as option_rows,
                countif(regexp_contains(trade_symbol, r'/')) as slash_symbols,
                countif(regexp_contains(coalesce(trade_symbol, ''), r' ')) as space_symbols,
                countif(
                    regexp_contains(
                        replace(upper(trim(coalesce(trade_symbol, ''))), ' ', ''),
                        r'^[A-Z][A-Z0-9.-]{{0,5}}\\d{{6}}[CP]\\d{{8}}$'
                    )
                    and not regexp_contains(coalesce(trade_symbol, ''), r'[ /]')
                ) as compact_symbols
            from {DS}.stg_history
            where instrument_type in ('Call', 'Put')
            """,
        ),
        (
            "check12",
            f"""
            with coach as (
                select coalesce(tenant_id, account) as tkey, strategy,
                       sum(total_closed) as n
                from {DS}.mart_coaching_signals
                group by 1, 2
            ),
            pos as (
                select coalesce(tenant_id, account) as tkey, strategy,
                       count(*) as n
                from {DS}.int_strategy_classification
                where trade_group_type = 'option_contract'
                  and status = 'Closed'
                group by 1, 2
            ),
            keys as (
                select tkey, strategy from coach
                union distinct
                select tkey, strategy from pos
            )
            select
                countif(
                    c.tkey is not null and p.tkey is not null
                    and c.n != p.n
                ) as mismatch_pairs,
                countif(c.tkey is not null and p.tkey is null) as coach_only_pairs,
                countif(
                    c.tkey is not null and p.tkey is not null
                    and c.n != p.n and c.strategy = 'Other Option'
                ) as mismatch_other_option,
                countif(
                    c.tkey is not null and p.tkey is not null
                    and c.n != p.n and c.strategy != 'Other Option'
                ) as mismatch_named,
                countif(
                    c.tkey is not null and p.tkey is not null
                    and c.n > p.n
                ) as coach_higher,
                countif(
                    c.tkey is not null and p.tkey is not null
                    and c.n < p.n
                ) as coach_lower
            from keys k
            left join coach c using (tkey, strategy)
            left join pos p using (tkey, strategy)
            """,
        ),
        (
            "extras",
            f"""
            with {_EXTRAS}
            select
                count(*) as extra_rows,
                countif(obh) as obh,
                countif(not obh) as not_obh,
                countif(user_id is null) as null_user,
                countif(direction = 'Sold') as dir_sold,
                countif(direction = 'Bought') as dir_bought,
                countif(direction = 'Unknown') as dir_unknown,
                countif(direction is null or direction not in ('Sold', 'Bought', 'Unknown')) as dir_other,
                countif(close_type = 'Assigned') as close_assigned,
                countif(close_type = 'Exercised') as close_exercised,
                countif(close_type = 'Expired') as close_expired,
                countif(close_type = 'Closed') as close_closed,
                countif(close_type = 'ExpiredOTM') as close_otm,
                countif(close_type = 'Settled at expiry (est.)') as close_est,
                countif(close_type = 'Settlement pending') as close_pending,
                countif(close_type is null) as close_null,
                countif(
                    close_type is not null
                    and close_type not in (
                        'Assigned', 'Exercised', 'Expired', 'Closed',
                        'ExpiredOTM', 'Settled at expiry (est.)', 'Settlement pending'
                    )
                ) as close_other,
                countif(close_date = current_date('America/New_York')) as dated_today,
                countif(option_expiry is not null and close_date = option_expiry) as dated_expiry,
                countif(option_type is null or option_type not in ('C', 'P')) as type_missing,
                countif(option_strike is null) as strike_missing,
                countif(option_expiry is null) as expiry_missing,
                countif(regexp_contains(coalesce(trade_symbol, ''), r'/')) as slash_symbols,
                countif(regexp_contains(coalesce(trade_symbol, ''), r' ')) as space_symbols,
                countif(starts_with(coalesce(tenant_id, ''), 'demo:')) as demo_rows,
                countif(lower(split(trim(coalesce(account, '')), ' ')[safe_offset(0)]) = 'schwab') as broker_schwab,
                countif(lower(split(trim(coalesce(account, '')), ' ')[safe_offset(0)]) = 'fidelity') as broker_fidelity,
                countif(lower(split(trim(coalesce(account, '')), ' ')[safe_offset(0)]) = 'vanguard') as broker_vanguard,
                countif(lower(split(trim(coalesce(account, '')), ' ')[safe_offset(0)]) = 'alpaca') as broker_alpaca,
                countif(lower(split(trim(coalesce(account, '')), ' ')[safe_offset(0)]) = 'interactive') as broker_interactive,
                countif(lower(split(trim(coalesce(account, '')), ' ')[safe_offset(0)]) not in
                        ('schwab', 'fidelity', 'vanguard', 'alpaca', 'interactive')) as broker_other
            from extras
            """,
        ),
        (
            "siblings",
            f"""
            with {_EXTRAS},
            opens as (
                select
                    h.tenant_id,
                    h.user_id,
                    h.trade_symbol,
                    h.underlying_symbol,
                    h.option_expiry,
                    h.option_strike,
                    h.option_type,
                    {canon_h} as canon,
                    countif(h.action in ('option_sell_to_open', 'option_buy_to_open')) as open_fills,
                    countif(regexp_contains(lower(coalesce(h.description, '')), r'est\\. fee')) as est_fee_rows
                from {DS}.stg_history h
                where h.instrument_type in ('Call', 'Put')
                group by 1, 2, 3, 4, 5, 6, 7, 8
            )
            select
                count(*) as extra_rows,
                countif(exists (
                    select 1 from opens o
                    where o.tenant_id = e.tenant_id
                      and o.trade_symbol = e.trade_symbol
                      and o.user_id is not distinct from e.user_id
                      and o.open_fills > 0
                )) as staged_open_same_symbol,
                countif(exists (
                    select 1 from opens o
                    where o.tenant_id = e.tenant_id
                      and o.canon = e.canon
                      and o.trade_symbol != e.trade_symbol
                      and o.open_fills > 0
                )) as staged_open_same_canon,
                countif(exists (
                    select 1 from opens o
                    where o.tenant_id = e.tenant_id
                      and o.option_expiry = e.option_expiry
                      and o.option_strike is not null
                      and e.option_strike is not null
                      and abs(o.option_strike - e.option_strike) < 0.001
                      and upper(trim(o.option_type)) = upper(trim(e.option_type))
                      and upper(trim(o.underlying_symbol)) = upper(trim(e.underlying_symbol))
                      and o.trade_symbol != e.trade_symbol
                      and o.open_fills > 0
                )) as staged_open_same_parts,
                countif(exists (
                    select 1 from opens o
                    where o.tenant_id = e.tenant_id
                      and o.option_expiry = e.option_expiry
                      and o.option_strike is not null
                      and e.option_strike is not null
                      and abs(o.option_strike - e.option_strike) < 0.001
                      and upper(trim(o.option_type)) = upper(trim(e.option_type))
                      and upper(trim(o.underlying_symbol)) != upper(trim(e.underlying_symbol))
                      and o.open_fills > 0
                )) as staged_open_diff_underlying,
                countif(exists (
                    select 1 from opens o
                    where o.tenant_id = e.tenant_id
                      and o.canon = e.canon
                      and o.open_fills > 0
                      and o.user_id is distinct from e.user_id
                )) as staged_open_other_user,
                countif(exists (
                    select 1 from {DS}.int_option_contracts c
                    where c.tenant_id = e.tenant_id
                      and c.trade_symbol != e.trade_symbol
                      and not coalesce(c.opened_before_history, false)
                      and c.option_expiry = e.option_expiry
                      and c.option_strike is not null
                      and e.option_strike is not null
                      and abs(c.option_strike - e.option_strike) < 0.001
                      and upper(trim(c.option_type)) = upper(trim(e.option_type))
                      and upper(trim(c.underlying_symbol)) = upper(trim(e.underlying_symbol))
                )) as classified_sibling_same_parts
            from extras e
            """,
        ),
        (
            "raw_opens",
            f"""
            with {_EXTRAS},
            raw_open as (
                select
                    nullif(trim(cast(r.tenant_id as string)), '') as tenant_id,
                    {raw_canon} as canon,
                    upper(trim(cast(r.Symbol as string))) as sym,
                    countif(regexp_contains(lower(coalesce(cast(r.Description as string), '')), r'est\\. fee')) as est_fee_rows
                from {RAW}.trade_history r
                where lower(trim(cast(r.Action as string))) in ('sell to open', 'buy to open')
                group by 1, 2, 3
            )
            select
                count(*) as extra_rows,
                countif(exists (
                    select 1 from raw_open o
                    where o.tenant_id = e.tenant_id
                      and o.sym = upper(trim(e.trade_symbol))
                )) as raw_open_same_text,
                countif(exists (
                    select 1 from raw_open o
                    where o.tenant_id = e.tenant_id
                      and o.canon = e.canon
                      and o.sym != upper(trim(e.trade_symbol))
                )) as raw_open_same_canon_other_text,
                countif(exists (
                    select 1 from raw_open o
                    where o.tenant_id = e.tenant_id
                      and o.canon = e.canon
                )) as raw_open_same_canon,
                countif(exists (
                    select 1 from raw_open o
                    where o.tenant_id = e.tenant_id
                      and o.canon = e.canon
                      and o.est_fee_rows > 0
                )) as raw_open_same_canon_est_fee,
                countif(not exists (
                    select 1 from raw_open o
                    where o.tenant_id = e.tenant_id
                      and o.canon = e.canon
                )) as raw_open_missing
            from extras e
            """,
        ),
        (
            "simulate",
            f"""
            with closed as (
                select
                    coalesce(oc.tenant_id, oc.account) as tkey,
                    coalesce(sc.strategy, 'Other Option') as strategy,
                    coalesce(oc.opened_before_history, false) as obh
                from {DS}.int_option_contracts oc
                left join {DS}.int_strategy_classification sc
                    on oc.account = sc.account
                    and oc.user_id is not distinct from sc.user_id
                    and oc.tenant_id is not distinct from sc.tenant_id
                    and oc.trade_symbol = sc.trade_symbol
                    and sc.trade_group_type = 'option_contract'
                where oc.status = 'Closed'
                  and oc.close_date is not null
            ),
            coach as (
                select tkey, strategy, countif(not obh) as n
                from closed
                group by 1, 2
            ),
            pos as (
                select coalesce(tenant_id, account) as tkey, strategy, count(*) as n
                from {DS}.int_strategy_classification
                where trade_group_type = 'option_contract'
                  and status = 'Closed'
                group by 1, 2
            ),
            keys as (
                select tkey, strategy from coach where n > 0
                union distinct
                select tkey, strategy from pos
            )
            select
                countif(
                    c.tkey is not null and p.tkey is not null
                    and coalesce(c.n, 0) != coalesce(p.n, 0)
                ) as remaining_mismatch_pairs,
                countif(c.tkey is not null and p.tkey is null and coalesce(c.n, 0) > 0) as remaining_coach_only
            from keys k
            left join coach c using (tkey, strategy)
            left join pos p using (tkey, strategy)
            """,
        ),
        (
            "grain",
            f"""
            with classified as (
                select
                    account,
                    user_id,
                    tenant_id,
                    trade_symbol,
                    strategy,
                    status
                from {DS}.int_strategy_classification
                where trade_group_type = 'option_contract'
            ),
            kept as (
                select
                    oc.account,
                    oc.user_id,
                    oc.tenant_id,
                    oc.trade_symbol,
                    oc.status,
                    oc.close_date,
                    sc.strategy
                from {DS}.int_option_contracts oc
                inner join classified sc
                    on oc.account = sc.account
                    and oc.user_id is not distinct from sc.user_id
                    and oc.tenant_id is not distinct from sc.tenant_id
                    and oc.trade_symbol = sc.trade_symbol
                where not coalesce(oc.opened_before_history, false)
            ),
            exit_rows as (
                select coalesce(tenant_id, account) as tkey, strategy
                from kept
                where status = 'Closed'
                  and close_date is not null
            ),
            kind_rows as (
                select coalesce(tenant_id, account) as tkey, strategy
                from kept
            ),
            roll_rows as (
                select coalesce(r.tenant_id, r.account) as tkey, sc.strategy
                from {DS}.int_option_rolls r
                inner join classified sc
                    on r.account = sc.account
                    and r.user_id is not distinct from sc.user_id
                    and r.tenant_id is not distinct from sc.tenant_id
                    and r.old_trade_symbol = sc.trade_symbol
            ),
            base as (
                select tkey, strategy from exit_rows
                union distinct
                select tkey, strategy from kind_rows
                union distinct
                select tkey, strategy from roll_rows
            ),
            coach as (
                select b.tkey, b.strategy, count(e.strategy) as n
                from base b
                left join exit_rows e using (tkey, strategy)
                group by 1, 2
            ),
            pos as (
                select coalesce(tenant_id, account) as tkey, strategy, count(*) as n
                from classified
                where status = 'Closed'
                group by 1, 2
            ),
            keys as (
                select tkey, strategy from coach
                union distinct
                select tkey, strategy from pos
            )
            select
                countif(
                    c.tkey is not null and p.tkey is not null
                    and coalesce(c.n, 0) != coalesce(p.n, 0)
                ) as mismatch_pairs,
                countif(c.tkey is not null and p.tkey is null) as coach_only_pairs,
                countif(
                    c.tkey is not null and p.tkey is null and coalesce(c.n, 0) = 0
                ) as coach_only_zero,
                countif(c.tkey is null and p.tkey is not null) as pos_only_pairs
            from keys k
            left join coach c using (tkey, strategy)
            left join pos p using (tkey, strategy)
            """,
        ),
        (
            "raw_actions",
            f"""
            with {_EXTRAS},
            raw_rows as (
                select
                    nullif(trim(cast(r.tenant_id as string)), '') as tenant_id,
                    {raw_canon} as canon,
                    case lower(trim(cast(r.Action as string)))
                        when 'sell to open' then 'sto'
                        when 'buy to open' then 'bto'
                        when 'sell to close' then 'stc'
                        when 'buy to close' then 'btc'
                        when 'expired' then 'expired'
                        when 'assigned' then 'assigned'
                        when 'exchange or exercise' then 'exercised'
                        when 'buy' then 'buy'
                        when 'sell' then 'sell'
                        when 'sell short' then 'sell_short'
                        else 'other'
                    end as act
                from {RAW}.trade_history r
            )
            select
                count(*) as extra_rows,
                countif(exists (
                    select 1 from raw_rows o
                    where o.tenant_id = e.tenant_id and o.canon = e.canon
                )) as any_raw,
                countif(exists (
                    select 1 from raw_rows o
                    where o.tenant_id = e.tenant_id and o.canon = e.canon and o.act in ('sto', 'bto')
                )) as raw_sto_bto,
                countif(exists (
                    select 1 from raw_rows o
                    where o.tenant_id = e.tenant_id and o.canon = e.canon and o.act in ('buy', 'sell', 'sell_short')
                )) as raw_equity_verb,
                countif(exists (
                    select 1 from raw_rows o
                    where o.tenant_id = e.tenant_id and o.canon = e.canon and o.act = 'expired'
                )) as raw_expired,
                countif(exists (
                    select 1 from raw_rows o
                    where o.tenant_id = e.tenant_id and o.canon = e.canon and o.act in ('stc', 'btc')
                )) as raw_close,
                countif(exists (
                    select 1 from raw_rows o
                    where o.tenant_id = e.tenant_id and o.canon = e.canon and o.act = 'other'
                )) as raw_other_action,
                countif(not exists (
                    select 1 from raw_rows o
                    where o.tenant_id = e.tenant_id
                )) as tenant_missing_from_raw
            from extras e
            """,
        ),
    ]


def assert_select_only(sql: str) -> None:
    stripped = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    stripped = re.sub(r"--.*?$", " ", stripped, flags=re.MULTILINE)
    if _FORBIDDEN.search(stripped):
        raise RuntimeError("refusing a non-select statement")
    head = stripped.lstrip().lower()
    if not (head.startswith("select") or head.startswith("with")):
        raise RuntimeError("statement does not start with select")


def emit(key: str, value) -> None:
    if value is None:
        token = "none"
    elif isinstance(value, bool):
        token = "yes" if value else "no"
    elif isinstance(value, float):
        token = str(int(round(value)))
    else:
        token = str(value)
    token = re.sub(r"[^A-Za-z0-9_%.-]", "_", token)
    line = f"{key}={token}"
    if not _LINE.match(line):
        print("line_dropped=1")
        return
    if re.search(r"snaptrade|tenant_id|[0-9a-f]{8}-[0-9a-f]{4}", line, re.I):
        print("line_dropped=1")
        return
    print(line)


def main() -> int:
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT)
    failed = 0
    for name, sql in queries():
        assert_select_only(sql)
        try:
            rows = list(client.query(sql).result())
        except Exception as exc:
            failed += 1
            kind = type(exc).__name__
            hint = "other"
            text = str(exc).lower()
            if "unrecognized name" in text or "not found" in text:
                hint = "missing_name"
            elif "syntax" in text:
                hint = "syntax"
            elif "access denied" in text or "permission" in text:
                hint = "permission"
            emit(name, f"ERROR_{hint}_{kind}")
            continue
        if not rows:
            emit(name, "empty")
            continue
        row = rows[0]
        for key, value in row.items():
            emit(f"{name}_{key}", value)
        if len(rows) != 1:
            emit(f"{name}_extra_result_rows", len(rows) - 1)
        if name == "grain":
            mismatch = int(row["mismatch_pairs"] or 0)
            coach_only = int(row["coach_only_pairs"] or 0)
            if mismatch or coach_only:
                failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
