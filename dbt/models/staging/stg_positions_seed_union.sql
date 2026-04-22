{{
    config(
        materialized='view'
    )
}}

select * from {{ ref('current_positions') }}
union all
select * from {{ ref('stg_schwab_open_positions_as_brokerage') }}
