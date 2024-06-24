SELECT
    user_id,
    product_id,
    COUNT(order_id) AS total_orders,
    MIN(order_number) AS min_order_number,
    MAX(order_number) AS max_order_number,
    AVG(add_to_cart_order) AS avg_add_to_cart_order
FROM order_products_prior
GROUP BY user_id, product_id;
