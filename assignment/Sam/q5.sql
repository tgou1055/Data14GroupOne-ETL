SELECT 
    product_id,
    COUNT(product_id) AS "count",
    SUM(reordered) AS "total reordered",
    SUM(CASE WHEN product_seq_time = 1 THEN 1 ELSE 0 END) AS "product_seq_time = 1",
    SUM(CASE WHEN product_seq_time = 2 THEN 1 ELSE 0 END) AS "product_seq_time = 2"
FROM (
    SELECT
        product_id,
        reordered,
        ROW_NUMBER() OVER (PARTITION BY user_id, product_id) AS product_seq_time
    FROM order_products_prior
) prod_seq
GROUP BY product_id;
