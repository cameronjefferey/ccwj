-- BQML: account_behavior_model
-- -------------------------------------------------------------
-- Global K-Means model trained on normalized per-account features.
-- Used as an anomaly detector via ML.DETECT_ANOMALIES.
-- Cluster IDs are intentionally never exposed to the app (unstable
-- across retrains).  Only `normalized_distance` (anomaly_score)
-- leaves the BQ layer.
--
-- Features are all ratios / counts (no absolute dollars) so a trader
-- at any size compares on the same scale.
--
-- Trained from mart_trade_observations after nightly dbt build.

CREATE OR REPLACE MODEL `ccwj-dbt.ml_models.account_behavior_model`
OPTIONS (
    model_type = 'kmeans',
    num_clusters = 8,
    standardize_features = TRUE,
    max_iterations = 50,
    kmeans_init_method = 'KMEANS++'
) AS
SELECT
    strategy,                                    -- BQML one-hot encodes STRING
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
  AND size_vs_30d_baseline BETWEEN 0.01 AND 20;
