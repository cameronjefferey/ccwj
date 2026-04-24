-- BQML: trade_anomaly_scores view
-- -------------------------------------------------------------
-- Wraps ML.DETECT_ANOMALIES against the global account_behavior_model.
-- Exposes only (account, trade_symbol, anomaly_score, is_anomaly) —
-- no cluster IDs.
--
-- contamination=0.03 = expect ~3% of closed trades to be flagged.
-- If too noisy, raise this threshold or filter by anomaly_score
-- downstream.

CREATE OR REPLACE VIEW `ccwj-dbt.ml_models.trade_anomaly_scores` AS
SELECT
    account,
    trade_symbol,
    is_anomaly,
    normalized_distance AS anomaly_score
FROM ML.DETECT_ANOMALIES(
    MODEL `ccwj-dbt.ml_models.account_behavior_model`,
    STRUCT(0.03 AS contamination),
    (
        SELECT
            account,
            trade_symbol,
            strategy,
            size_vs_30d_baseline,
            size_vs_90d_baseline,
            cast(consecutive_losses_before as float64) as consecutive_losses_before,
            cast(coalesce(days_since_last_loss, 0) as float64) as days_since_last_loss,
            cast(day_of_week as float64)               as day_of_week,
            coalesce(strategy_win_rate_180d, 0.5)      as strategy_win_rate_180d
        FROM `ccwj-dbt.analytics.mart_trade_observations`
        WHERE close_date IS NOT NULL
          AND size_vs_30d_baseline IS NOT NULL
          AND size_vs_90d_baseline IS NOT NULL
          AND size_vs_90d_baseline BETWEEN 0.01 AND 20
          AND size_vs_30d_baseline BETWEEN 0.01 AND 20
    )
);
