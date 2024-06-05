select 
    user_id, 
    max(order_number) as "max order number", 
    sum(days_since_prior_order) as "sum of days since prior order", 
    avg(days_since_prior_order) as "avg of days since prior order" 
from orders
group by user_id;