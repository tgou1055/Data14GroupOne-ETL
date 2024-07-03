SELECT user_id, 
count(*) product_id, 
count(distinct product_id), 
sum(case when reordered = 1 then 1 else 0 end) / cast(sum(case when order_number > 1 then 1 else 0 end as double)
from order_products_prior 
group by user_id;