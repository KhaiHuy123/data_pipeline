select *
from {{ source('bronze_layer', 'bronze_product_category_name_translation') }}