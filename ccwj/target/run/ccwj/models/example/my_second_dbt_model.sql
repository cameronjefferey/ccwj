

  create or replace view `ccwj-dbt`.`analytics`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `ccwj-dbt`.`analytics`.`my_first_dbt_model`
where id = 1;

