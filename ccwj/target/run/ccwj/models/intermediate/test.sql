

  create or replace view `ccwj-dbt`.`analytics`.`test`
  OPTIONS()
  as select distinct action
from `ccwj-dbt`.`analytics`.`history`;

