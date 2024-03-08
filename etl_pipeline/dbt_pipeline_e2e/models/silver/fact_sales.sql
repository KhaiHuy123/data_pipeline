-- fact sales
select
    ood.order_id , ood.customer_id , ooid.product_id ,
    ood.order_purchase_timestamp, oopd.payment_value,
    ood.order_status
from {{ref("raw_orders")}} as ood
join {{ref("raw_order_items")}} as ooid
on ood.order_id  = ooid.order_id
join {{ref("raw_order_payments")}} as oopd
on oopd.order_id = ooid.order_id


