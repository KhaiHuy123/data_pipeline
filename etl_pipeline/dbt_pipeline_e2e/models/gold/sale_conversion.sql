-- sale_conversion
with
sale_conversion as (
	select count(distinct fc.customer_id) as customers_purchased,
	doc.order_condition_stats as all_customers,
	fc.order_status
	from {{ref('fact_customers')}} fc
	inner join {{ref('dim_orders_condition')}} doc
	on fc.order_status = doc.order_status
	group by fc.order_status, doc.order_condition_stats
	)
select * from sale_conversion
-- select
-- sum(customers_purchased) as customers_purchased,
-- sum(all_customers) as all_customers,
-- sum(customers_purchased)/sum(all_customers) as sale_conversion_rate
-- from sale_conversion