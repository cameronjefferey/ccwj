select * 
from {{ ref('current')}}
where symbol not in ('Cash', 'Account')