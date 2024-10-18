select 
    *
from {{ ref('current')}}
where symbol like 'Account%'