-- dim products
select
    opd.product_id,
    pct.product_category_name_english
from {{ref("raw_products")}} as opd
join {{ref("raw_categories")}} as pct
on pct.product_category_name_english = opd.product_category_name

