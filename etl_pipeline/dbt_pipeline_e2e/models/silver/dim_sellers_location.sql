-- dim_sellers
select
osd.seller_id, osd.seller_city , osd.seller_zip_code_prefix ,
count(osd.seller_city) as num_of_sellers
from {{ref("raw_sellers")}} osd
group by osd.seller_id, osd.seller_city, osd.seller_zip_code_prefix
