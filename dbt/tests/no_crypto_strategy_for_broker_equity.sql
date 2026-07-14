{#
    Ticker-collision regression (2026-07-14).

    Several crypto tickers COLLIDE with real equities/ETFs — the canonical
    case is ``SEI``: both the Sei token AND Solaris Energy Infrastructure
    (NYSE). A user held Solaris on Schwab (an equity-only broker) and the
    share was classified under the ``Crypto`` strategy purely because the
    ticker was on the crypto whitelist (``stg_crypto_symbols``), rendering a
    bogus "Crypto" row on an equity + long-call position.

    The fix makes the ``Crypto`` label broker-corroborated: an equity
    session is Crypto only when the broker itself reports the holding as
    crypto (``stg_current.security_type_raw='Cryptocurrency'``), or when the
    ticker is whitelisted AND the broker does NOT report it as a
    conventional equity/ETF. A holding the broker calls a stock/ETF must
    NEVER be classified Crypto, whatever its ticker.

    Fails (returns rows) if any position classified ``Crypto`` corresponds
    to a CURRENT broker holding the broker reports as a conventional
    equity/ETF. (Closed positions aren't in stg_current and legitimately
    fall back to the whitelist, so they're out of scope here.)
#}

with crypto_classified as (
    select tenant_id, account, user_id, symbol
    from {{ ref('int_strategy_classification') }}
    where strategy = 'Crypto'
),

broker_equity as (
    select distinct
        tenant_id,
        account,
        user_id,
        upper(trim(underlying_symbol)) as symbol
    from {{ ref('stg_current') }}
    where instrument_type = 'Equity'
      and lower(coalesce(security_type_raw, '')) in ('equity', 'etfs & closed end funds')
)

select
    c.tenant_id,
    c.account,
    c.user_id,
    c.symbol
from crypto_classified c
join broker_equity b
    on c.account = b.account
    and (c.user_id is not distinct from b.user_id)
    and (c.tenant_id is not distinct from b.tenant_id)
    and upper(trim(c.symbol)) = b.symbol
