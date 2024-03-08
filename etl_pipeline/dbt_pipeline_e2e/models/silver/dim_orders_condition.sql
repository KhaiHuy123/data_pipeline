-- dim_orders_conditions
select count(ood2.order_status) as order_condition_stats, ood2.order_status
from {{ ref("raw_orders") }} ood2
group by ood2.order_status