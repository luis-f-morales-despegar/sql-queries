Select *
from raw.cartera_b2b_v1
where 1=1
and ag_name not like '%Test%'
and agency_code <> 'agency_code'
order by director desc