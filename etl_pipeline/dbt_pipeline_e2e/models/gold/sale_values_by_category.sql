-- sale_values_by_category
with
	daily_sales_product as (
    select
    fs2.order_purchase_timestamp::date as daily,
    fs2.product_id,
    sum(fs2.payment_value) as sales,
    count(distinct fs2.order_id) as orders
    from {{ref("fact_sales")}} fs2
    group by order_purchase_timestamp::date, fs2.product_id
),
	monthly_sales_categories as(
	select
	to_char(dl.daily, 'YYYY-MM') as monthly ,
	dp.product_category_name_english  as category,
	dl.sales, dl.orders
	from daily_sales_product as dl
	join {{ref("dim_products")}} as dp
	on dl.product_id = dp.product_id
)
select monthly, category,
round(cast(sum(sales)as numeric), 1) as total_sales,
sum(orders) as total_orders,
round(cast(sum(sales)/ sum(orders) as numeric), 2) as value_per_order
from monthly_sales_categories
group by monthly, category

