-- fact_customers
select
    ocd.customer_id ,
    count(oopd.order_id) as num_of_orders ,
    oopd.payment_value as value_per_order ,
    round(sum(cast(oopd.payment_value as numeric)), 2) as all_payment_value,
    ood.order_status, oopd.payment_type,
    ood.order_purchase_timestamp
from {{ ref("raw_customers") }} ocd
join {{ ref("raw_orders") }} ood
on ocd.customer_id = ood.customer_id
join {{ ref("raw_order_payments") }} oopd
on ood.order_id = oopd.order_id
group by ocd.customer_id ,oopd.payment_value, ood.order_status,
oopd.payment_type, ood.order_purchase_timestamp, oopd.order_id
having count(ocd.customer_id) >= 1
order by ood.order_purchase_timestamp asc

