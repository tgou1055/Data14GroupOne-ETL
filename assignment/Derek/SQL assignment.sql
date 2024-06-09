
 
-- Q1. Create a table called order_products_prior by using the last SQL query you created from the previous assignment. It should be similar to below (note you need to replace the s3 bucket n_a_m_e_ _“i_m_b_a_” _t_o_ _y_o_u_r_s_ _o_w_n_ _b_u_c_k_e_t_ _n_a_m_e_)_:_ _

CREATE TABLE order_products_prior AS (SELECT a.*, b.product_id, b.add_to_cart_order, b.reordered FROM orders a JOIN order_products b ON a.order_id = b.order_id WHERE a.eval_set = 'prior') 

-- Q2. Create a SQL query (user_features_1). Based on table orders, for each user, calculate the max order_number, the sum of days_since_prior_order and the average of days_since_prior_order. 

SELECT user_id, max(order_number) AS MAX_order_number, 
       CAST(SUM(days_since_prior_order) AS INT) AS SUM_days_since_prior_order, 
       ROUND(AVG(days_since_prior_order),2) AVG_days_since_prior_order
FROM prod.orders
GROUP BY user_id
LIMIT 100



-- Q3. Create a SQL query (user_features_2). Similar to above, based on table order_products_prior, for each user calculate the total number of products, total number of distinct products, and user reorder ratio(number of reordered = 1 divided by number of order_number > 1) 

SELECT user_id, 
    COUNT(product_id) AS total_number_of_products, 
    COUNT(DISTINCT product_id) AS number_of_distinct_product,
    SUM(CASE WHEN reordered = 1 THEN 1 ELSE 0 END) * 1.0 /
    COUNT(CASE WHEN order_number > 1 THEN 1 ELSE NULL END) AS re_order_ratio
FROM prod.order_products_prior
GROUP BY user_id 
LIMIT 10;



-- Q4. Create a SQL query (up_features). Based on table order_products_prior, for each user and product, calculate the total number of orders, minimum order_number, maximum order_number and average add_to_cart_order 

	
SELECT user_id, product_id,
        COUNT(order_id) AS total_number_of_orders,
        MIN(order_number) AS min_order_number,
        MAX(order_number) AS Max_order_number,
        ROUND(AVG(add_to_cart_order),1) AS avg_add_to_cart_order
FROM order_products_prior 
GROUP BY user_id, product_id
LIMIT 50;
 
-- Q5. Create a SQL query (prd_features). Based on table order_products_prior, 
-- first write a sql query to calculate the sequence of product purchase for each user, 
-- and name it product_seq_time (_F_o_r_ _e_x_a_m_p_l_e_,_ _i_f_ _a_ _u_s_e_r_ _f_i_r_s_t_ _t_i_m_e_ _p_u_r_c_h_a_s_e_ _a_ _p_r_o_d_u_c_t_ _A_,_ _m_a_r_k_ _i_t_ _a_s_ _1_._ _I_f_ _i_t_’s_ _the second time a user purchases a product A, mark it as 2). Below are some examples: 

-- Q5.1 CREATE A VIEW to store the result for further use

CREATE VIEW product_with_Sequence_number AS
SELECT user_id,order_number, op.product_id, product_name,reordered,
       ROW_NUMBER() OVER (PARTITION BY user_id,p.product_id ORDER BY user_id) AS sequence_number
FROM order_products_prior op
JOIN products p 
ON op.product_id = p.product_id
ORDER BY user_id;



-- Q5.2 Then on top of this query, for each product, calculate the count, sum of reordered, count of product_seq_time = 1 and count of product_seq_time = 2.
-- each product, calculate the count, sum of reordered.count of product_seq_time = 1 and count of product_seq_time = 2.

SELECT product_id,
       COUNT(reordered) AS reordered_count,
       SUM(reordered) AS reordered_SUM,
       SUM(CASE WHEN sequence_number = 1 THEN 1 ELSE 0 END) AS sequence_number_1_count,
       SUM(CASE WHEN sequence_number = 2 THEN 1 ELSE 0 END) AS sequence_number_2_count
FROM product_with_sequence_number
GROUP BY product_id
ORDER BY product_id ASC;

-- version 08/06/2024
-- v.2.1



