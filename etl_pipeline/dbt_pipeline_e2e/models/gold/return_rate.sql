-- return_rate
with
customers_info as (
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
    having count(ocd.customer_id) > 1
    and ood.order_status not in ('unavailable', 'canceled')
    order by ood.order_purchase_timestamp asc
),
return_rate as(
    select
    count(rc.customer_id) as return_customers,
    doc.order_condition_stats as all_customers,
    rc.order_status
    from customers_info rc
    inner join {{ref('dim_orders_condition')}} doc
    on doc.order_status  = rc.order_status
    group by doc.order_condition_stats, rc.order_status
),
return_rate_results as (
	select
	return_customers,
	all_customers ,
	(return_customers/all_customers) as return_rate,
	order_status
	from return_rate
)
select * from return_rate_results
-- select
-- sum(return_customer) as return_customer,
-- sum(all_customer) as all_customer,
-- sum((return_customer/all_customer)) as return_rate
-- from return_rate_results

