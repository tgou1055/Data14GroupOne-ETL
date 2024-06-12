SELECT 
    product_id,
    COUNT(product_id) AS total_products,
    SUM(reordered) AS total_reordered,
    SUM(CASE WHEN product_seq_time = 1 THEN 1 ELSE 0 END) AS product_seq_time_is_1,
    SUM(CASE WHEN product_seq_time = 2 THEN 1 ELSE 0 END) AS product_seq_time_is_2
FROM (
    SELECT
        product_id,
        reordered,
        ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY order_number ASC) AS product_seq_time
    FROM order_products_prior
) prod_seq
GROUP BY product_id;
