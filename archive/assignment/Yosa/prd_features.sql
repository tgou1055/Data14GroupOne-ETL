SELECT product_id, count(*), sum(reordered), 
sum(CASE WHEN product_seq_time = 1 THEN 1 ELSE 0 END),
sum(CASE WHEN product_seq_time = 2 THEN 1 ELSE 0 END)
from (select *, rank()
over (
partition by user_id, product_id
order by order_number)
from order_products_prior)