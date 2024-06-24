SELECT user_id, product_id, count(*), min(order_number), max(order_number),avg(add_to_cart_order)
from order_products_prior
group by user_id, product_id