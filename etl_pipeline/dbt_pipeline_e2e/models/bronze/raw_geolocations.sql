select *
from {{ source('bronze_layer', 'bronze_olist_geolocation_dataset') }}