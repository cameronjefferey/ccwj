-- BQML: account_trade_insights view
-- -------------------------------------------------------------
-- Joins deterministic observation text from mart_trade_observations
-- with the BQML anomaly score.  Downstream (Flask) reads this view
-- scoped by account.
--
-- Only surfaces rows where observation_text is non-null, so the BQML
-- anomaly_score acts purely as a *ranking* on top of the deterministic
-- signal — never fires an alert the deterministic layer wouldn't have
-- independently made.

CREATE OR REPLACE VIEW `ccwj-dbt.ml_models.account_trade_insights` AS
SELECT
    m.account,
    m.trade_symbol,
    m.underlying_symbol,
    m.strategy,
    m.open_date,
    m.close_date,
    m.notional_proxy,
    m.size_vs_30d_baseline,
    m.size_vs_90d_baseline,
    m.strategy_win_rate_180d,
    m.strategy_prior_trades_180d,
    m.consecutive_losses_before,
    m.days_since_last_loss,
    m.observation_text,
    coalesce(a.anomaly_score, 0.0)                   AS anomaly_score,
    coalesce(a.is_anomaly, FALSE)                    AS is_anomaly
FROM `ccwj-dbt.analytics.mart_trade_observations` m
LEFT JOIN `ccwj-dbt.ml_models.trade_anomaly_scores` a
    ON m.account = a.account
    AND m.trade_symbol = a.trade_symbol
WHERE m.observation_text IS NOT NULL;
