CREATE TABLE order_products_prior AS 
(
    SELECT a.*, b.product_id, b.add_to_cart_order, b.reordered 
    FROM orders a 
    JOIN order_product b ON a.order_id = b.order_id 
    WHERE a.eval_set = 'prior'
);

/*Q2. Create a SQL query (user_features_1). Based on table orders, for each user, calculate the max order_number, the sum of days_since_prior_order and the average of days_since_prior_order */
SELECT
        USER_ID,
        MAX(ORDER_NUMBER) AS max_order_number,
        SUM(DAYS_SINCE_PRIOR_ORDER) AS sum_days_since_prior_order,
        AVG(DAYS_SINCE_PRIOR_ORDER) AS avg_days_since_prior_order
    FROM
        orders
    GROUP BY
        USER_ID

/*Q3. Create a SQL query (user_features_2). Similar to above, based on table order_products_prior, for each user calculate the total number of products, total number of distinct products, and user reorder ratio(number of reordered = 1 divided by number of order_number > 1) */
SELECT
        a.USER_ID,
        COUNT(b.PRODUCT_ID) AS total_products,
        COUNT(DISTINCT b.PRODUCT_ID) AS total_distinct_products,
        SUM(CASE WHEN b.REORDERED = 1 THEN 1 ELSE 0 END) * 1.0 / 
        COUNT(CASE WHEN a.ORDER_NUMBER > 1 THEN 1 ELSE NULL END) AS user_reorder_ratio
    FROM
        orders a
    JOIN
        order_products_prior b ON a.ORDER_ID = b.ORDER_ID
    GROUP BY
        a.USER_ID

/*Q4. Create a SQL query (up_features). Based on table order_products_prior, for each user and product, calculate the total number of orders, minimum order_number, maximum order_number and average add_to_cart_order*/
SELECT
        a.USER_ID,
        b.PRODUCT_ID,
        COUNT(b.ORDER_ID) AS total_orders,
        MIN(a.ORDER_NUMBER) AS min_order_number,
        MAX(a.ORDER_NUMBER) AS max_order_number,
        AVG(b.ADD_TO_CART_ORDER) AS avg_add_to_cart_order
    FROM
        orders a
    JOIN
        order_products_prior b ON a.ORDER_ID = b.ORDER_ID
    GROUP BY
        a.USER_ID, b.PRODUCT_ID

/*Q5. Create a SQL query (prd_features). Based on table order_products_prior, first write a SQL query to calculate the sequence of product purchase for each user, and name it product_seq_time (For example, if a user first time purchase a product A, mark it as 1. If it’s the second time a user purchases a product A, mark it as 2).*/
-- Step 1: Create a CTE to calculate the sequence of product purchase for each user
WITH product_seq_time AS (
    SELECT
        a.USER_ID,
        b.PRODUCT_ID,
        a.ORDER_ID,
        ROW_NUMBER() OVER (PARTITION BY a.USER_ID, b.PRODUCT_ID ORDER BY a.ORDER_NUMBER) AS purchase_sequence
    FROM
        orders a
    JOIN
        order_products_prior b ON a.ORDER_ID = b.ORDER_ID
)

-- Step 2: Select from the CTE to verify the results
SELECT * FROM product_seq_time;