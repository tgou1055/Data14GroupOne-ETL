SELECT
    user_id,
    COUNT(product_id) AS "total number of products", 
    COUNT(DISTINCT product_id) AS "total number of distinct products", 
    SUM(CASE WHEN reordered = 1 THEN 1 ELSE 0 END) * 1.0 / 
    SUM(CASE WHEN order_number > 1 THEN 1 ELSE 0 END) AS "reorder ratio"
FROM order_products_prior
GROUP BY user_id;
