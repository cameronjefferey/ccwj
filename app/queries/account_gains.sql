SELECT 
    account, 
    sum(unrealized_gain_or_loss) as unrealized_gain_or_loss, 
    sum(realized_gain_or_loss) as realized_gain_or_loss
FROM `ccwj-dbt.analytics.account_gains`
WHERE DATE(transaction_date) BETWEEN @start_date AND @end_date
group by 1
