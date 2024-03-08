select *
from {{ source('bronze_layer', 'bronze_olist_sellers_dataset') }}