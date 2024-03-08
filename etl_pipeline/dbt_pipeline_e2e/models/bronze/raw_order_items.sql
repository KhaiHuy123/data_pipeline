select *
from {{ source('bronze_layer', 'bronze_olist_order_items_dataset') }}