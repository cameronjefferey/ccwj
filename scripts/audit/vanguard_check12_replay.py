"""Read-only replay of CHECK 12 after the Vanguard verb map.

Simulates two unbuilt changes on the current warehouse:

- The staging map (this branch): the first Vanguard Buy or Sell on a
  call or put, by date then seed sequence, becomes the open. Later
  fills in the other direction become the close. Contracts that
  already have an option open are unchanged.
- The coach filter (this branch): a contract that is still
  opened-before-history is dropped from the coach, which is the same
  population classification already uses.

No dbt build. No seed write. Every statement is a SELECT. The log
prints counts only.
"""
from __future__ import annotations

import re
import sys

PROJECT = "ccwj-dbt"
DS = f"`{PROJECT}.analytics`"
RAW = f"`{PROJECT}.analytics_raw`"

_LINE = re.compile(r"^[a-z][a-z0-9_]*=[A-Za-z0-9_%.-]+$")
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|create|drop|alter|truncate|grant|revoke)\b",
    re.IGNORECASE,
)


def _canon(expiry, strike, cp, und, sym) -> str:
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


def _raw_canon(sym: str) -> str:
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
        coalesce(
            nullif(regexp_extract({s}, r'^([A-Z][A-Z0-9.-]{{0,5}})\\s+\\d{{1,2}}/'), ''),
            nullif(regexp_extract({s}, r'^([A-Z][A-Z0-9.-]{{0,5}})\\d{{6}}[CP]'), '')
        )
    """
    return _canon(expiry, strike, cp, und, s)


def _date(expr: str) -> str:
    cell = f"trim(cast({expr} as string))"
    return f"""coalesce(
        safe.parse_date('%m/%d/%Y', regexp_extract({cell}, r'(\\d{{1,2}}/\\d{{1,2}}/\\d{{4}})')),
        safe.parse_date('%Y-%m-%d', regexp_extract({cell}, r'(\\d{{4}}-\\d{{2}}-\\d{{2}})')),
        safe.parse_date('%m/%d/%y', regexp_extract({cell}, r'^(\\d{{1,2}}/\\d{{1,2}}/\\d{{2}})(?:\\s|$)'))
    )"""


def _replay_sql() -> str:
    raw_canon = _raw_canon("r.Symbol")
    return f"""
    with extras as (
        select
            oc.tenant_id,
            oc.user_id,
            oc.trade_symbol,
            coalesce(oc.opened_before_history, false) as obh,
            lower(split(trim(coalesce(oc.account, '')), ' ')[safe_offset(0)]) = 'vanguard'
                as is_vanguard
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
    ),
    raw_verbs as (
        select
            nullif(trim(cast(r.tenant_id as string)), '') as tenant_id,
            {raw_canon} as canon,
            case lower(trim(cast(r.Action as string)))
                when 'buy' then 'buy'
                when 'sell' then 'sell'
            end as verb,
            {_date('r.Date')} as trade_date,
            cast(r._row_seq as int64) as row_seq
        from {RAW}.trade_history r
        where lower(split(trim(coalesce(cast(r.Account as string), '')), ' ')[safe_offset(0)])
                = 'vanguard'
          and lower(trim(cast(r.Action as string))) in ('buy', 'sell')
    ),
    first_verb as (
        select tenant_id, canon, verb as first_verb
        from (
            select
                tenant_id,
                canon,
                verb,
                row_number() over (
                    partition by tenant_id, canon
                    order by trade_date nulls last, row_seq nulls last
                ) as verb_ord
            from raw_verbs
            where verb is not null
              and canon is not null
        )
        where verb_ord = 1
    ),
    staged_open as (
        select distinct
            h.tenant_id,
            h.trade_symbol
        from {DS}.stg_history h
        where h.action in ('option_buy_to_open', 'option_sell_to_open')
    ),
    judged as (
        select
            e.obh,
            e.is_vanguard,
            f.first_verb,
            (
                e.is_vanguard
                and f.first_verb in ('buy', 'sell')
                and o.trade_symbol is null
            ) as will_classify
        from extras e
        left join first_verb f
            on f.tenant_id = e.tenant_id
           and f.canon = e.trade_symbol
        left join staged_open o
            on o.tenant_id = e.tenant_id
           and o.trade_symbol = e.trade_symbol
    ),
    extra_counts as (
        select
            count(*) as extras,
            countif(obh) as extras_obh,
            countif(is_vanguard) as extras_vanguard,
            countif(not is_vanguard) as extras_other_broker,
            countif(will_classify) as now_classify,
            countif(not will_classify) as still_dropped,
            countif(will_classify and first_verb = 'buy') as classify_buy_first,
            countif(will_classify and first_verb = 'sell') as classify_sell_first,
            countif(will_classify and not obh) as classify_not_currently_obh
        from judged
    ),
    coach as (
        select
            coalesce(tenant_id, account) as tkey,
            strategy,
            sum(total_closed) as closed_in_coach
        from {DS}.mart_coaching_signals
        group by 1, 2
    ),
    classified as (
        select
            coalesce(tenant_id, account) as tkey,
            strategy,
            count(*) as closed_in_pos
        from {DS}.int_strategy_classification
        where trade_group_type = 'option_contract'
          and status = 'Closed'
        group by 1, 2
    ),
    gap as (
        select
            coalesce(c.closed_in_coach, 0) as coach_n,
            coalesce(p.closed_in_pos, 0) as pos_n
        from coach c
        full outer join classified p
            on c.tkey = p.tkey
           and c.strategy = p.strategy
        where coalesce(c.closed_in_coach, 0) != coalesce(p.closed_in_pos, 0)
           or (c.tkey is not null and p.tkey is null)
    ),
    gap_counts as (
        select
            count(*) as gap_keys,
            countif(coach_n > pos_n) as gap_coach_heavy_keys,
            countif(pos_n > coach_n) as gap_classified_heavy_keys,
            countif(coach_n = 0 and pos_n = 0) as gap_zero_keys,
            coalesce(sum(coach_n - pos_n), 0) as gap_contract_delta
        from gap
    )
    select
        x.extras,
        x.extras_obh,
        x.extras_vanguard,
        x.extras_other_broker,
        x.now_classify,
        x.still_dropped,
        x.classify_buy_first,
        x.classify_sell_first,
        x.classify_not_currently_obh,
        g.gap_keys,
        g.gap_coach_heavy_keys,
        g.gap_classified_heavy_keys,
        g.gap_zero_keys,
        g.gap_contract_delta,
        case
            when x.extras = x.still_dropped + x.now_classify
             and x.now_classify = x.classify_buy_first + x.classify_sell_first
             and x.classify_not_currently_obh = 0
             and x.extras_obh = x.extras
             and g.gap_contract_delta = x.extras
             and g.gap_classified_heavy_keys = 0
             and g.gap_zero_keys = 0
            then 0
            else -1
        end as mismatch_after
    from extra_counts x
    cross join gap_counts g
    """


def queries():
    return [("replay", _replay_sql())]


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


def _mismatch_failed(value) -> bool:
    """Zero is a clean replay. None is not."""
    if value is None:
        return True
    return int(value) != 0


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
            text = str(exc).lower()
            hint = "other"
            if "unrecognized name" in text or "not found" in text:
                hint = "missing_name"
            elif "syntax" in text:
                hint = "syntax"
            elif "access denied" in text or "permission" in text:
                hint = "permission"
            emit(name, f"ERROR_{hint}_{type(exc).__name__}")
            continue
        if len(rows) != 1:
            emit(name, "bad_shape")
            failed += 1
            continue
        for key, value in rows[0].items():
            emit(f"{name}_{key}", value)
        if _mismatch_failed(rows[0]["mismatch_after"]):
            failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
