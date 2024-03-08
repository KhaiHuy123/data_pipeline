
-- return nothing means test OK

select
osd.seller_city , count(osd.seller_city) as count_city_info,
osd.seller_state , count(osd.seller_state) as count_state_info
from {{ source('bronze_layer', 'bronze_olist_sellers_dataset') }} osd
where length(osd.seller_city) < 1
group by osd.seller_city, osd.seller_state

