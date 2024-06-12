SELECT user_id, max(order_number), sum(days_since_prior_order), avg(days_since_prior_order) from orders
group by user_id