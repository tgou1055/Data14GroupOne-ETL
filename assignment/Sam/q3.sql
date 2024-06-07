SELECT
    user_id,
    COUNT(product_id) AS total_products_count,
    COUNT(DISTINCT product_id) AS total_distinct_products_count, 
    SUM(CASE WHEN reordered = 1 THEN 1 ELSE 0 END) * 1.0 / 
    SUM(CASE WHEN order_number > 1 THEN 1 ELSE 0 END) AS reorder_ratio
FROM order_products_prior
GROUP BY user_id;
