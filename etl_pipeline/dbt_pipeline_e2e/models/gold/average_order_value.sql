-- average_order_value
with
    average_order_value as (
    select
    sum(fc.num_of_orders) as total_orders,
    round(sum(fc.all_payment_value),2) as total_revenue,
    round(sum(fc.all_payment_value)/ sum(fc.num_of_orders), 2) as average_order_value,
    fc.order_status
    from {{ref('fact_customers')}} fc
    inner join {{ref('dim_orders_condition')}} doc
    on fc.order_status = doc.order_status
    group by fc.order_status
)
select * from average_order_value
-- select
-- sum(total_orders) as total_orders,
-- sum(total_revenue) as total_revenue,
-- sum(average_order_value) as average_order_value
-- from average_order_value
