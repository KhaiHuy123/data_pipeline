-- dim_customers
select
ocd.customer_id , ocd.customer_city , ocd.customer_zip_code_prefix ,
count(ocd.customer_city) as num_of_customers
from {{ref("raw_customers")}} ocd
group by ocd.customer_id, ocd.customer_city, ocd.customer_zip_code_prefix
