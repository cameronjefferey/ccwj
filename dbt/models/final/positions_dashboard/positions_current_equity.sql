/* Used as source for positions dashboard */

select 
    *
from {{ ref('positions_metadata')}}