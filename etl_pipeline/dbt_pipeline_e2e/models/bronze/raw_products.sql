select *
from {{ source('bronze_layer', 'bronze_olist_products_dataset') }}