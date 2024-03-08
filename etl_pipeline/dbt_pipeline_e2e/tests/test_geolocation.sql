
-- return nothing means test OK

select
ogd.geolocation_city , count(ogd.geolocation_city) as count_geolocation_info,
ogd.geolocation_state , count(ogd.geolocation_state) as count_state_info
from {{ source('bronze_layer', 'bronze_olist_geolocation_dataset') }} ogd
where length(ogd.geolocation_city) < 1
group by ogd.geolocation_city, ogd.geolocation_state

