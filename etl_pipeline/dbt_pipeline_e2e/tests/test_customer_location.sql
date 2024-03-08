
-- return nothing means test OK

select
ocd.customer_city , count(ocd.customer_city) as count_city_info,
ocd.customer_state , count(ocd.customer_state) as count_state_info
from {{ source('bronze_layer', 'bronze_olist_customers_dataset') }} ocd
where length(ocd.customer_city) < 1
group by ocd.customer_city , ocd.customer_state

