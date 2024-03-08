
-- return nothing means test OK

select
count(ooid.price) as price_0, ooid.price
from
{{ source('bronze_layer', 'bronze_olist_order_items_dataset') }} ooid
group by ooid.price
having  ooid.price = 0


