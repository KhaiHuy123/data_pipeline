select *
from {{ source('bronze_layer', 'bronze_olist_orders_dataset') }}