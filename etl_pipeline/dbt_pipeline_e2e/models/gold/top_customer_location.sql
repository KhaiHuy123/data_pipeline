-- top_customer_location
select
cl.customer_id, cl.customer_city,
cl.customer_zip_code_prefix, num_of_customers,
fc.num_of_orders,
(fc.all_payment_value / fc.num_of_orders) as average_order_value,
fc.order_status, fc.payment_type, fc.order_purchase_timestamp
from {{ref("dim_customers_location")}} cl
join {{ref("fact_customers")}} fc
on cl.customer_id = fc.customer_id
order by  average_order_value desc

