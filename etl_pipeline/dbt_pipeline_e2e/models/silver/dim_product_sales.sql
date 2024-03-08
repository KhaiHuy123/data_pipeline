-- dim_product_sales
select
    opd.product_id, opd.product_category_name,
    ooid.order_id , ooid.order_item_id, ooid.seller_id ,
    (ood.order_estimated_delivery_date::date -  ood.order_delivered_customer_date::date) as delivered_within_days ,
    round(cast(ooid.price as numeric), 1) as price , ooid.freight_value,
    oopd.payment_type, oopd.payment_value
from {{ref("raw_products")}} opd
join {{ref("raw_order_items")}} ooid
on ooid.product_id = opd.product_id
join {{ref("raw_order_payments")}} oopd
on oopd.order_id = ooid.order_id
join {{ref("raw_orders")}} ood
on ood.order_id = oopd.order_id
order by ooid.price desc


