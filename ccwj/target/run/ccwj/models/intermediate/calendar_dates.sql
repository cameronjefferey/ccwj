

  create or replace view `ccwj-dbt`.`analytics`.`calendar_dates`
  OPTIONS()
  as SELECT day
FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE('2020-01-01'), date_add(current_date(),INTERVAL 90 DAY), INTERVAL 1 DAY)
) AS day;

