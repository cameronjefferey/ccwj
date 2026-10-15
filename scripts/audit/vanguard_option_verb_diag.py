"""Read-only profile of Vanguard Buy/Sell rows on CHECK 12 contracts.

CHECK 12 extras are closed option contracts with no staged opening fill
(``opened_before_history``). This script asks whether a raw Buy or Sell
on that same contract is a real open that staging stored as an equity
verb, or a row that is not an open.

Prints counts and category shares only. Account ids, tickers, prices,
and dollar amounts never leave BigQuery. Every statement is a SELECT.
"""
from __future__ import annotations

import sys
from pathlib import Path

# ``python scripts/audit/...`` puts this directory on sys.path, not the repo
# root. Pytest already has the root. Either way the sibling module loads.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.audit.check12_gap import _canon, _raw_canon, assert_select_only, emit

PROJECT = "ccwj-dbt"
DS = f"`{PROJECT}.analytics`"
RAW = f"`{PROJECT}.analytics_raw`"

# Quantity match is a hundredth of a contract. The per-share premium
# identity allows the same per-contract fee budget the day page uses
# when a premium is gross. The per-contract identity allows a 2% gap.
QTY_TOL = 0.01
X100_FEE_FLOOR = 1.0
X100_FEE_PER = 1.5
X1_ABS = 0.05
X1_REL = 0.02

REAL_OPEN_KINDS = (
    "real_open_x100",
    "real_open_x1",
    "real_open_mixed",
    "real_open_round_trip",
)
PRE_HISTORY_KINDS = (
    "no_verb_before_close",
    "share_delivery",
)
AMBIGUOUS_KINDS = (
    "before_price_other",
    "before_qty_mismatch",
    "undated",
    "closed_qty_zero",
)
ALL_KINDS = REAL_OPEN_KINDS + ("same_day_open",) + PRE_HISTORY_KINDS + AMBIGUOUS_KINDS


def _num(expr: str) -> str:
    return f"""safe_cast(
        nullif(
            regexp_replace(
                replace(replace(trim(cast({expr} as string)), ',', ''), '$', ''),
                r'^\\((.*)\\)$',
                r'-\\1'
            ),
            ''
        ) as float64
    )"""


def _date(expr: str) -> str:
    cell = f"trim(cast({expr} as string))"
    return f"""coalesce(
        safe.parse_date(
            '%m/%d/%Y',
            regexp_extract({cell}, r'(?i)\\bas of\\s+(\\d{{1,2}}/\\d{{1,2}}/\\d{{4}})')
        ),
        safe.parse_date(
            '%m/%d/%Y',
            regexp_extract({cell}, r'(\\d{{1,2}}/\\d{{1,2}}/\\d{{4}})')
        ),
        safe.parse_date(
            '%Y-%m-%d',
            regexp_extract({cell}, r'(\\d{{4}}-\\d{{2}}-\\d{{2}})')
        ),
        safe.parse_date(
            '%m/%d/%y',
            regexp_extract({cell}, r'^(\\d{{1,2}}/\\d{{1,2}}/\\d{{2}})(?:\\s|$)')
        )
    )"""


def _verb(action: str) -> str:
    return f"""case lower(trim(cast({action} as string)))
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
    end"""


def _shape(sym: str) -> str:
    compact = r"^[A-Z][A-Z0-9.\-]{0,5}\d{6}[CP]\d{8}$"
    return (
        "case "
        f"when regexp_contains(replace({sym}, ' ', ''), r'{compact}') "
        f"and not regexp_contains({sym}, r'[ /]') then 'compact' "
        f"when regexp_contains({sym}, r'\\d{{6}}[CP]\\d{{8}}') then 'osi_tail' "
        f"when regexp_contains({sym}, r'\\d{{1,2}}/\\d{{1,2}}/\\d{{2,4}}') then 'long_form' "
        "else 'other' end"
    )


def _extras_sql() -> str:
    canon = _canon(
        "oc.option_expiry",
        "oc.option_strike",
        "oc.option_type",
        "oc.underlying_symbol",
        "oc.trade_symbol",
    )
    return f"""
    extras as (
        select
            oc.tenant_id,
            oc.user_id,
            oc.trade_symbol,
            oc.direction,
            oc.close_type,
            oc.close_date,
            coalesce(oc.contracts_closed, 0) as contracts_closed,
            coalesce(oc.opened_before_history, false) as obh,
            lower(split(trim(coalesce(oc.account, '')), ' ')[safe_offset(0)]) = 'vanguard'
                as is_vanguard,
            {canon} as canon
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


def _raw_sql() -> str:
    sym = "r.Symbol"
    qty = f"abs(coalesce({_num('r.Quantity')}, 0))"
    price = f"abs(coalesce({_num('r.Price')}, 0))"
    amount = f"abs(coalesce({_num('r.Amount')}, 0))"
    gap100 = f"abs({amount} - {qty} * {price} * 100)"
    gap1 = f"abs({amount} - {qty} * {price})"
    desc = "r.Description"
    return f"""
    raw_rows as (
        select
            nullif(trim(cast(r.tenant_id as string)), '') as tenant_id,
            {_raw_canon(sym)} as canon,
            {_verb('r.Action')} as verb,
            {_date('r.Date')} as trade_date,
            {qty} as qty,
            {gap100} <= greatest({X100_FEE_FLOOR}, {X100_FEE_PER} * {qty})
                and {gap100} <= {gap1}
                and {qty} > 0
                and {price} > 0 as opt_px,
            {gap1} <= greatest({X1_ABS}, {X1_REL} * {qty} * {price})
                and {gap1} < {gap100}
                and {qty} > 0
                and {price} > 0 as eq_px,
            {_shape("upper(trim(cast(r.Symbol as string)))")} as shape,
            regexp_contains(lower(coalesce(cast({desc} as string), '')), r'to open') as desc_to_open,
            regexp_contains(lower(coalesce(cast({desc} as string), '')), r'to close') as desc_to_close,
            regexp_contains(
                lower(coalesce(cast({desc} as string), '')),
                r'\\b(call|put)\\b'
            ) as desc_call_put,
            regexp_contains(lower(coalesce(cast({desc} as string), '')), r'option') as desc_option_word,
            trim(coalesce(cast({desc} as string), '')) = '' as desc_blank
        from {RAW}.trade_history r
    )
    """


def _population_sql() -> str:
    return f"""
    with {_extras_sql()},
    {_raw_sql()},
    flagged as (
        select
            e.obh,
            e.is_vanguard,
            exists (
                select 1 from raw_rows b
                where b.tenant_id = e.tenant_id
                  and b.canon = e.canon
                  and b.verb in ('buy', 'sell')
            ) as has_buy_sell,
            exists (
                select 1 from raw_rows b
                where b.tenant_id = e.tenant_id
                  and b.canon = e.canon
                  and b.verb = 'sell_short'
            ) as has_sell_short
        from extras e
    )
    select
        count(*) as extras,
        countif(obh) as obh,
        countif(obh and is_vanguard) as vanguard_obh,
        countif(obh and is_vanguard and has_buy_sell) as vanguard_buy_or_sell,
        countif(obh and is_vanguard and has_sell_short and not has_buy_sell) as vanguard_sell_short_only,
        countif(obh and not is_vanguard and has_buy_sell) as other_broker_buy_or_sell
    from flagged
    """


def _verbs_sql() -> str:
    """One row of counts. ``kind`` matches ``bucket()`` in this file."""
    tol = QTY_TOL
    return f"""
    with {_extras_sql()},
    {_raw_sql()},
    cohort as (
        select e.*
        from extras e
        where e.obh
          and e.is_vanguard
          and exists (
                select 1 from raw_rows b
                where b.tenant_id = e.tenant_id
                  and b.canon = e.canon
                  and b.verb in ('buy', 'sell')
          )
    ),
    per as (
        select
            c.tenant_id,
            c.user_id,
            c.canon,
            c.trade_symbol,
            c.close_date,
            c.contracts_closed,
            c.direction,
            c.close_type,
            countif(r.verb in ('buy', 'sell')) as n_verb,
            countif(r.verb in ('buy', 'sell') and r.trade_date is not null) as n_verb_dated,
            countif(r.verb in ('buy', 'sell') and r.trade_date < c.close_date) as n_before,
            countif(r.verb in ('buy', 'sell') and r.trade_date = c.close_date) as n_same,
            countif(r.verb in ('buy', 'sell') and r.trade_date > c.close_date) as n_after,
            countif(r.verb in ('expired', 'assigned', 'exercised', 'stc', 'btc')) as n_proper_close,
            coalesce(sum(if(
                r.verb = 'sell' and r.trade_date < c.close_date and r.opt_px, r.qty, 0
            )), 0) as opt_sell_before,
            coalesce(sum(if(
                r.verb = 'buy' and r.trade_date < c.close_date and r.opt_px, r.qty, 0
            )), 0) as opt_buy_before,
            coalesce(sum(if(
                r.verb = 'sell' and r.trade_date < c.close_date and r.eq_px, r.qty, 0
            )), 0) as eq_sell_before,
            coalesce(sum(if(
                r.verb = 'buy' and r.trade_date < c.close_date and r.eq_px, r.qty, 0
            )), 0) as eq_buy_before,
            coalesce(sum(if(
                r.verb in ('buy', 'sell') and r.trade_date < c.close_date, r.qty, 0
            )), 0) as qty_before,
            coalesce(sum(if(
                r.verb in ('buy', 'sell') and r.trade_date = c.close_date and r.opt_px, r.qty, 0
            )), 0) as opt_qty_same,
            coalesce(sum(if(
                r.verb in ('buy', 'sell') and r.trade_date = c.close_date and r.eq_px, r.qty, 0
            )), 0) as eq_qty_same,
            countif(r.verb in ('buy', 'sell')) > 0
                and countif(r.verb in ('buy', 'sell'))
                    = countif(r.verb in ('buy', 'sell') and r.shape = 'compact')
                as shape_all_compact,
            countif(r.verb in ('buy', 'sell') and r.shape = 'long_form') > 0 as shape_any_long_form,
            countif(r.verb in ('buy', 'sell') and r.shape = 'osi_tail') > 0 as shape_any_osi_tail,
            countif(r.verb in ('buy', 'sell') and r.shape = 'other') > 0 as shape_any_other,
            countif(r.verb in ('buy', 'sell') and r.desc_to_open) > 0 as desc_to_open,
            countif(r.verb in ('buy', 'sell') and r.desc_to_close) > 0 as desc_to_close,
            countif(r.verb in ('buy', 'sell') and r.desc_call_put) > 0 as desc_call_put,
            countif(r.verb in ('buy', 'sell') and r.desc_option_word) > 0 as desc_option_word,
            countif(r.verb in ('buy', 'sell')) > 0
                and countif(r.verb in ('buy', 'sell'))
                    = countif(r.verb in ('buy', 'sell') and r.desc_blank)
                as desc_all_blank,
            min(if(
                r.verb = 'buy' and r.trade_date < c.close_date and (r.opt_px or r.eq_px),
                r.trade_date, null
            )) as first_buy,
            min(if(
                r.verb = 'sell' and r.trade_date < c.close_date and (r.opt_px or r.eq_px),
                r.trade_date, null
            )) as first_sell
        from cohort c
        join raw_rows r
            on r.tenant_id = c.tenant_id
           and r.canon = c.canon
        group by
            c.tenant_id, c.user_id, c.canon, c.trade_symbol, c.close_date,
            c.contracts_closed, c.direction, c.close_type
    ),
    labeled as (
        select
            p.*,
            (p.opt_sell_before + p.eq_sell_before) as sell_like,
            (p.opt_buy_before + p.eq_buy_before) as buy_like,
            (p.opt_sell_before + p.opt_buy_before) as opt_before,
            (p.eq_sell_before + p.eq_buy_before) as eq_before,
            (p.opt_qty_same + p.eq_qty_same) as like_same
        from per p
    ),
    kinded as (
        select
            l.*,
            case
                when l.contracts_closed <= {tol} then 'closed_qty_zero'
                when l.n_verb > 0 and l.n_verb_dated = 0 then 'undated'
                when l.n_before = 0
                 and l.n_same > 0
                 and abs(l.like_same - l.contracts_closed) <= {tol}
                 and l.n_proper_close > 0
                then 'same_day_open'
                when l.n_before = 0 then 'no_verb_before_close'
                when abs(l.sell_like - l.contracts_closed) <= {tol}
                 and abs(l.buy_like - l.contracts_closed) <= {tol}
                then 'real_open_round_trip'
                when abs(l.opt_before - l.contracts_closed) <= {tol}
                 and (
                        l.eq_before <= {tol}
                     or abs(l.eq_before - l.contracts_closed * 100)
                            <= greatest(1.0, 0.01 * l.contracts_closed * 100)
                 )
                then 'real_open_x100'
                when abs(l.eq_before - l.contracts_closed) <= {tol}
                 and l.opt_before <= {tol}
                then 'real_open_x1'
                when abs(l.opt_before + l.eq_before - l.contracts_closed) <= {tol}
                 and l.opt_before > {tol}
                 and l.eq_before > {tol}
                then 'real_open_mixed'
                when l.opt_before <= {tol}
                 and abs(l.eq_before - l.contracts_closed * 100)
                        <= greatest(1.0, 0.01 * l.contracts_closed * 100)
                then 'share_delivery'
                when abs(l.qty_before - l.contracts_closed) <= {tol}
                then 'before_price_other'
                else 'before_qty_mismatch'
            end as kind
        from labeled l
    )
    select
        count(*) as cohort,
        countif(kind = 'real_open_x100') as real_open_x100,
        countif(kind = 'real_open_x1') as real_open_x1,
        countif(kind = 'real_open_mixed') as real_open_mixed,
        countif(kind = 'real_open_round_trip') as real_open_round_trip,
        countif(kind = 'same_day_open') as same_day_open,
        countif(kind = 'no_verb_before_close') as no_verb_before_close,
        countif(kind = 'share_delivery') as share_delivery,
        countif(kind = 'before_price_other') as before_price_other,
        countif(kind = 'before_qty_mismatch') as before_qty_mismatch,
        countif(kind = 'undated') as undated,
        countif(kind = 'closed_qty_zero') as closed_qty_zero,
        countif(n_before > 0) as dated_before,
        countif(abs(qty_before - contracts_closed) <= {tol}) as qty_match_before,
        countif(opt_before + eq_before > {tol}) as option_price_before,
        countif(kind in (
            'real_open_x100', 'real_open_x1', 'real_open_mixed', 'real_open_round_trip'
        )) as aligned_real_open,
        countif(kind in (
            'real_open_x100', 'real_open_x1', 'real_open_mixed', 'real_open_round_trip'
        ) and n_proper_close > 0) as real_open_with_proper_close,
        countif(kind in (
            'real_open_x100', 'real_open_x1', 'real_open_mixed', 'real_open_round_trip'
        ) and n_same > 0) as real_open_also_verb_on_close,
        countif(sell_like > {tol} and buy_like <= {tol}) as only_sell_like_before,
        countif(buy_like > {tol} and sell_like <= {tol}) as only_buy_like_before,
        countif(sell_like > {tol} and buy_like > {tol}) as both_sides_like_before,
        countif(n_proper_close > 0) as proper_close,
        countif(direction = 'Sold') as dir_sold,
        countif(direction = 'Bought') as dir_bought,
        countif(direction = 'Unknown') as dir_unknown,
        countif(direction is null or direction not in ('Sold', 'Bought', 'Unknown')) as dir_other,
        countif(close_type = 'Expired') as close_expired,
        countif(close_type = 'Closed') as close_closed,
        countif(close_type = 'Assigned') as close_assigned,
        countif(close_type = 'Exercised') as close_exercised,
        countif(
            close_type is null
            or close_type not in ('Expired', 'Closed', 'Assigned', 'Exercised')
        ) as close_other,
        countif(shape_all_compact) as shape_all_compact,
        countif(shape_any_long_form) as shape_any_long_form,
        countif(shape_any_osi_tail) as shape_any_osi_tail,
        countif(shape_any_other) as shape_any_other,
        countif(desc_to_open) as desc_to_open,
        countif(desc_to_close) as desc_to_close,
        countif(desc_call_put) as desc_call_put,
        countif(desc_option_word) as desc_option_word,
        countif(desc_all_blank) as desc_all_blank,
        countif(exists (
            select 1
            from {DS}.stg_history h
            where h.tenant_id = kinded.tenant_id
              and (h.trade_symbol = kinded.canon or h.trade_symbol = kinded.trade_symbol)
              and h.instrument_type in ('Call', 'Put')
              and h.action in ('equity_buy', 'equity_sell', 'equity_sell_short')
              and h.trade_date < kinded.close_date
        )) as staged_equity_verb_before,
        countif(exists (
            select 1
            from {DS}.stg_history h
            where h.tenant_id = kinded.tenant_id
              and (h.trade_symbol = kinded.canon or h.trade_symbol = kinded.trade_symbol)
              and h.action in ('option_buy_to_open', 'option_sell_to_open')
        )) as staged_already_open,
        countif(
            sell_like > {tol} and buy_like > {tol} and first_buy < first_sell
        ) as both_buy_earlier,
        countif(
            sell_like > {tol} and buy_like > {tol} and first_sell < first_buy
        ) as both_sell_earlier,
        countif(
            sell_like > {tol} and buy_like > {tol} and first_buy = first_sell
        ) as both_same_day,
        countif(
            sell_like > {tol} and buy_like > {tol}
            and first_buy < first_sell
            and abs(buy_like - (sell_like + contracts_closed)) <= {tol}
        ) as both_long_partial,
        countif(
            sell_like > {tol} and buy_like > {tol}
            and first_sell < first_buy
            and abs(sell_like - (buy_like + contracts_closed)) <= {tol}
        ) as both_short_partial,
        countif(
            sell_like > {tol} and buy_like > {tol}
            and first_buy < first_sell
            and abs(buy_like - sell_like) <= {tol}
            and abs(buy_like - (sell_like + contracts_closed)) > {tol}
        ) as both_long_same_qty,
        countif(
            sell_like > {tol} and buy_like > {tol}
            and first_sell < first_buy
            and abs(sell_like - buy_like) <= {tol}
            and abs(sell_like - (buy_like + contracts_closed)) > {tol}
        ) as both_short_same_qty,
        countif(
            sell_like > {tol} and buy_like > {tol}
            and first_buy < first_sell
            and abs(buy_like - contracts_closed) <= {tol}
            and abs(buy_like - sell_like) > {tol}
            and abs(buy_like - (sell_like + contracts_closed)) > {tol}
        ) as both_long_matches_expiry,
        countif(
            sell_like > {tol} and buy_like > {tol}
            and not (
                (first_buy < first_sell and (
                    abs(buy_like - (sell_like + contracts_closed)) <= {tol}
                    or abs(buy_like - sell_like) <= {tol}
                    or abs(buy_like - contracts_closed) <= {tol}
                ))
                or (first_sell < first_buy and (
                    abs(sell_like - (buy_like + contracts_closed)) <= {tol}
                    or abs(sell_like - buy_like) <= {tol}
                ))
                or first_buy = first_sell
                or first_buy is null
                or first_sell is null
            )
        ) as both_other
    from kinded
    """


def _book_sql() -> str:
    """All Vanguard option rows staged as equity verbs, not only the 105."""
    return f"""
    with verbs as (
        select
            h.tenant_id,
            h.trade_symbol,
            countif(h.action = 'equity_buy') as n_buy,
            countif(h.action = 'equity_sell') as n_sell,
            countif(h.action = 'equity_sell_short') as n_short
        from {DS}.stg_history h
        where h.instrument_type in ('Call', 'Put')
          and lower(split(trim(coalesce(h.account, '')), ' ')[safe_offset(0)]) = 'vanguard'
          and h.action in ('equity_buy', 'equity_sell', 'equity_sell_short')
        group by 1, 2
    ),
    status as (
        select
            tenant_id,
            trade_symbol,
            max(status) as status
        from {DS}.int_option_contracts
        where lower(split(trim(coalesce(account, '')), ' ')[safe_offset(0)]) = 'vanguard'
        group by 1, 2
    )
    select
        count(*) as symbols,
        countif(n_buy > 0 and n_sell = 0 and n_short = 0) as only_buy,
        countif(n_sell > 0 and n_buy = 0 and n_short = 0) as only_sell,
        countif(n_short > 0 and n_buy = 0 and n_sell = 0) as only_short,
        countif(n_buy > 0 and n_sell > 0) as both_buy_and_sell,
        countif(n_short > 0 and (n_buy > 0 or n_sell > 0)) as short_with_other,
        countif(n_sell > 0 and n_buy = 0 and n_short = 0 and s.status = 'Open') as only_sell_open,
        countif(n_sell > 0 and n_buy = 0 and n_short = 0 and s.status = 'Closed') as only_sell_closed,
        countif(n_buy > 0 and n_sell = 0 and n_short = 0 and s.status = 'Open') as only_buy_open,
        countif(n_buy > 0 and n_sell = 0 and n_short = 0 and s.status = 'Closed') as only_buy_closed,
        countif(n_buy > 0 and n_sell > 0 and s.status = 'Open') as both_open,
        countif(n_buy > 0 and n_sell > 0 and s.status = 'Closed') as both_closed
    from verbs v
    left join status s
        on v.tenant_id = s.tenant_id
       and v.trade_symbol = s.trade_symbol
    """


def queries():
    return [
        ("population", _population_sql()),
        ("verbs", _verbs_sql()),
        ("book", _book_sql()),
    ]


def price_flags(qty: float, price: float, amount: float) -> tuple[bool, bool]:
    """Per-share (×100) and per-contract (×1) premium identities.

    The two are exclusive: the closer cash identity wins. A non-positive
    quantity or price is neither.
    """
    qty = abs(qty)
    price = abs(price)
    amount = abs(amount)
    if qty <= 0 or price <= 0:
        return False, False
    gap100 = abs(amount - qty * price * 100)
    gap1 = abs(amount - qty * price)
    opt_px = gap100 <= max(X100_FEE_FLOOR, X100_FEE_PER * qty) and gap100 <= gap1
    eq_px = gap1 <= max(X1_ABS, X1_REL * qty * price) and gap1 < gap100
    return opt_px, eq_px


def bucket(stats: dict) -> str:
    """Exclusive category. Same order as the ``kind`` case in ``_verbs_sql``."""
    closed = stats["contracts_closed"]
    if closed <= QTY_TOL:
        return "closed_qty_zero"
    if stats["n_verb"] > 0 and stats["n_verb_dated"] == 0:
        return "undated"
    like_same = stats["opt_qty_same"] + stats["eq_qty_same"]
    if (
        stats["n_before"] == 0
        and stats["n_same"] > 0
        and abs(like_same - closed) <= QTY_TOL
        and stats["n_proper_close"] > 0
    ):
        return "same_day_open"
    if stats["n_before"] == 0:
        return "no_verb_before_close"
    sell_like = stats["opt_sell_before"] + stats["eq_sell_before"]
    buy_like = stats["opt_buy_before"] + stats["eq_buy_before"]
    if abs(sell_like - closed) <= QTY_TOL and abs(buy_like - closed) <= QTY_TOL:
        return "real_open_round_trip"
    opt_before = stats["opt_sell_before"] + stats["opt_buy_before"]
    eq_before = stats["eq_sell_before"] + stats["eq_buy_before"]
    share_gap = abs(eq_before - closed * 100)
    share_tol = max(1.0, 0.01 * closed * 100)
    if abs(opt_before - closed) <= QTY_TOL and (
        eq_before <= QTY_TOL or share_gap <= share_tol
    ):
        return "real_open_x100"
    if abs(eq_before - closed) <= QTY_TOL and opt_before <= QTY_TOL:
        return "real_open_x1"
    if (
        abs(opt_before + eq_before - closed) <= QTY_TOL
        and opt_before > QTY_TOL
        and eq_before > QTY_TOL
    ):
        return "real_open_mixed"
    if opt_before <= QTY_TOL and share_gap <= share_tol:
        return "share_delivery"
    if abs(stats["qty_before"] - closed) <= QTY_TOL:
        return "before_price_other"
    return "before_qty_mismatch"


def _blank(**overrides) -> dict:
    base = {
        "contracts_closed": 1.0,
        "n_verb": 1,
        "n_verb_dated": 1,
        "n_before": 1,
        "n_same": 0,
        "n_after": 0,
        "n_proper_close": 1,
        "opt_sell_before": 0.0,
        "opt_buy_before": 0.0,
        "eq_sell_before": 0.0,
        "eq_buy_before": 0.0,
        "qty_before": 0.0,
        "opt_qty_same": 0.0,
        "eq_qty_same": 0.0,
    }
    base.update(overrides)
    return base


def _share(part: int, whole: int) -> str:
    if whole <= 0:
        return "none"
    return f"{int(round(100.0 * part / whole))}%"


def _emit_shares(row) -> None:
    cohort = int(row["cohort"] or 0)
    real = sum(int(row[name] or 0) for name in REAL_OPEN_KINDS)
    pre = sum(int(row[name] or 0) for name in PRE_HISTORY_KINDS)
    ambiguous = sum(int(row[name] or 0) for name in AMBIGUOUS_KINDS)
    same = int(row["same_day_open"] or 0)
    emit("verbs_real_open", real)
    emit("verbs_real_open_share", _share(real, cohort))
    emit("verbs_pre_history", pre)
    emit("verbs_pre_history_share", _share(pre, cohort))
    emit("verbs_same_day", same)
    emit("verbs_same_day_share", _share(same, cohort))
    emit("verbs_ambiguous", ambiguous)
    emit("verbs_ambiguous_share", _share(ambiguous, cohort))
    emit("verbs_partition", real + pre + same + ambiguous)


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
            hint = "other"
            text = str(exc).lower()
            if "unrecognized name" in text or "not found" in text:
                hint = "missing_name"
            elif "syntax" in text:
                hint = "syntax"
            elif "access denied" in text or "permission" in text:
                hint = "permission"
            emit(name, f"ERROR_{hint}_{type(exc).__name__}")
            continue
        if not rows:
            emit(name, "empty")
            failed += 1
            continue
        row = rows[0]
        for key, value in row.items():
            emit(f"{name}_{key}", value)
        if len(rows) != 1:
            emit(f"{name}_extra_result_rows", len(rows) - 1)
            failed += 1
        if name == "verbs":
            _emit_shares(row)
            cohort = int(row["cohort"] or 0)
            part = sum(int(row[kind] or 0) for kind in ALL_KINDS)
            if part != cohort:
                failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
