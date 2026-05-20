{{
    config(materialized='view')
}}

-- Canonical list of symbols we classify as Crypto.
--
-- Source of truth: ``dbt/seeds/crypto_symbols.csv``. This view exists
-- so downstream models join to a normalized (uppercase, trimmed)
-- ``symbol`` regardless of how the seed CSV is shaped, and so we have
-- one place to layer in additional crypto signals later (e.g. unioning
-- in ``stg_current.security_type='Cryptocurrency'`` once SnapTrade
-- syncs start emitting that marker — see app/snaptrade_normalize.py).
--
-- Why a curated whitelist instead of detecting per-row from the seed
-- CSV's ``security_type`` column: existing Coinbase data ALREADY
-- landed in the seed with ``security_type='Equity'`` (SnapTrade
-- normalize used to lump every non-option into Equity), so retro-
-- classifying that data needs a symbol-based decision somewhere. The
-- whitelist also gives an admin a single auditable place to add a new
-- ticker when a user starts holding e.g. SUI without changing any
-- Python or SQL.
--
-- Runtime mirror: ``app/upload.py:CRYPTO_SYMBOLS`` carries the same
-- list for fast in-process checks (e.g. ``_compute_breakdown_by_type``
-- per-symbol gate without a BQ round-trip). A pytest pins the two
-- in sync — see ``tests/test_snaptrade_normalize.py``.

select
    upper(trim(symbol))      as symbol,
    coalesce(nullif(trim(asset_class), ''), 'Crypto') as asset_class
from {{ ref('crypto_symbols') }}
where trim(coalesce(symbol, '')) != ''
