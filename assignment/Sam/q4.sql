SELECT
    user_id,
    product_id,
    COUNT(order_id) AS "total number of orders",
    MIN(order_number) AS "min order number",
    MAX(order_number) AS "max order number",
    AVG(add_to_cart_order) AS "avg add to cart order"
FROM order_products_prior
GROUP BY user_id, product_id;
