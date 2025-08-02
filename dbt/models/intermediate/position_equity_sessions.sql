WITH equity_trades AS (
  SELECT
    account,
    symbol,
    transaction_date,
    case when action = 'sell' then -quantity else quantity end AS quantity,
    action,
    amount
  FROM {{ ref('history_and_current_combined')}}
  WHERE security_type = 'Equity'
),

running AS (
  SELECT
    *,
    SUM(quantity) OVER (
      PARTITION BY account, symbol
      ORDER BY transaction_date, action desc 
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_qty
  FROM equity_trades
),

with_prev AS (
  SELECT
    *,
    LAG(running_qty, 1, 0) OVER (
      PARTITION BY account, symbol
      ORDER BY transaction_date
    ) AS prev_running_qty
  FROM running
),

sessions AS (
  SELECT
    *,
    -- increment only when you're transitioning from 0 → positive
    SUM(
      IF(prev_running_qty = 0 AND running_qty > 0, 1, 0)
    ) OVER (
      PARTITION BY account, symbol
      ORDER BY transaction_date, action desc 
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_id
  FROM with_prev
)

SELECT
  account,
  symbol,
  transaction_date,
  quantity,
  action,
  amount,
  session_id,

  -- overall trade order within the session
  ROW_NUMBER() OVER (
    PARTITION BY account, symbol, session_id
    ORDER BY transaction_date, action desc
  ) AS equity_trade_order,

  -- order of buys in that session (NULL on sells)
  CASE WHEN action = 'buy' THEN
    ROW_NUMBER() OVER (
      PARTITION BY account, symbol, session_id
      ORDER BY transaction_date, action desc 
    )
  END AS equity_trade_buy_order,

  -- order of sells in that session (NULL on buys)
  CASE WHEN action = 'sell' THEN
    ROW_NUMBER() OVER (
      PARTITION BY account, symbol, session_id
      ORDER BY transaction_date
    )
  END AS equity_trade_sell_order

FROM sessions
where 1=1