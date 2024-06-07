select 
    user_id, 
    max(order_number) as max_order_number, 
    sum(days_since_prior_order) as sum_days_since_prior_order, 
    avg(days_since_prior_order) as avg_days_since_prior_order
from orders
group by user_id;
