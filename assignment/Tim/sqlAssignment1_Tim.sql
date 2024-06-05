
/* 
Q.1
    create a table that connects the prior 
*/

CREATE TABLE order_products_prior AS
(SELECT a.*, 
	b.product_id, 
	b.add_to_cart_order, 
	b.reordered
FROM orders a
JOIN order_products b
ON a.order_id = b.order_id
WHERE a.eval_set = 'prior')

/* count how many entries in order_products_prior*/
SELECT COUNT(*) FROM order_products_prior;

/*
Q2
    Create a SQL query (user_features_1). Based on table orders, for each user, calculate the
    max order_number, the sum of days_since_prior_order and the average of days_since_prior_order.
*/

/* Examine the table */
SELECT * FROM orders ORDER BY user_id, order_number LIMIT 50;

/* We should consider whether using integer or float for the sum_days_prior and avg_days_prior */

SELECT user_id,
       MAX(order_number) as max_order_number, 
       CAST(SUM(days_since_prior_order) AS INT) as sum_days_prior,
       ROUND(AVG(days_since_prior_order),2) as avg_days_prior
FROM orders 
GROUP BY user_id 
ORDER BY user_id;


/*
Q3.
    Create a SQL query (user_features_2). Similar to above, based on table
    order_products_prior, for each user calculate the total number of products, total number of
    distinct products, and user reorder ratio(number of reordered = 1 divided by number of
    order_number > 1)
*/


WITH user_ratio AS (SELECT user_id, 
			   COUNT(*) as product_bought, 
                           COUNT(DISTINCT(product_id)) as unique_product_bought, 
			   COUNT(CASE WHEN reordered = 1 THEN 1 ELSE NULL END) as num_reordered, 
		           COUNT(CASE WHEN order_number > 1 THEN 1 ELSE NULL END) as num_order_number
		    FROM order_products_prior
		    GROUP BY user_id
		    ORDER BY user_id) SELECT user_id, 
 					     product_bought, 
					     unique_product_bought, 
					     num_reordered, num_order_number, 
					     ROUND(CAST(num_reordered AS DOUBLE) / num_order_number ,4) AS reorder_ratio 
				      FROM user_ratio


/*
Q4:
    Create a SQL query (up_features). Based on table order_products_prior, for each user and
    product, calculate the total number of orders, minimum order_number, maximum
    order_number and average add_to_cart_order.
*/

SELECT user_id, 
       product_id, 
       COUNT(*) as num_of_orders, 
       MIN(order_number) as min_order_num, 
       MAX(order_number) as max_order_num, 
       ROUND(AVG(add_to_cart_order),2) as seq_add_to_order
FROM order_products_prior
GROUP BY user_id, product_id
ORDER BY user_id, product_id;


/*
Q5:
    Create a SQL query (prd_features). Based on table order_products_prior, first write a sql
    query to calculate the sequence of product purchase for each user, and name it
    product_seq_time (For example, if a user first time purchase a product A, mark it as 1. If it’s
    the second time a user purchases a product A, mark it as 2)
    
    Then on top of this query, for each product, calculate the count, sum of reordered, count of
    product_seq_time = 1 and count of product_seq_time = 2.
*/

WITH product_seq AS (SELECT user_id, 
			    order_number, 
			    product_id,
			    ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY order_number ASC) AS product_seq_time,
			    reordered
		     FROM order_products_prior
		     ORDER BY user_id, order_number, product_seq_time) SELECT product_id, 
									      COUNT(*) AS num_product_ordered, 
									      SUM(reordered) as sum_reordered, 
									      COUNT(CASE WHEN product_seq_time = 1 THEN 1 ELSE NULL END) as seq_is_one, 
									      COUNT(CASE WHEN product_seq_time = 2 THEN 1 ELSE NULL END) as seq_is_two
									      FROM product_seq
									      GROUP BY product_id
									      ORDER BY product_id;
