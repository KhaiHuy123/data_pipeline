

-- return nothing means test OK

select
    ocd.customer_id ,
    oopd.order_id ,
    count(ocd.customer_id) as num_of_orders ,
    oopd.payment_value as value_per_order,
    round(sum(cast(oopd.payment_value as numeric)), 2) as all_payment_value,
    ood.order_status, oopd.payment_type,
    ood.order_purchase_timestamp
from {{ source('bronze_layer', 'bronze_olist_customers_dataset') }} ocd
join {{ source('bronze_layer', 'bronze_olist_orders_dataset') }} ood
on ocd.customer_id = ood.customer_id
join {{ source('bronze_layer', 'bronze_olist_order_payments_dataset') }} oopd
on ood.order_id = oopd.order_id
group by ocd.customer_id ,oopd.payment_value, ood.order_status,
oopd.payment_type, ood.order_purchase_timestamp, oopd.order_id
having count(ocd.customer_id) = 0 or count(oopd.order_id) = 0
order by ood.order_purchase_timestamp asc

