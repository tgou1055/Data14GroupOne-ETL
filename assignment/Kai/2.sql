CREATE TABLE user_features_1 WITH (
	external_location = 's3://weikaibucket/features/user_features_1/',
	format = 'PARQUET'
) AS
SELECT user_id,
	MAX(order_number) AS max_order_number,
	SUM(days_since_prior_order) AS sum_days_since_prior_order,
	AVG(days_since_prior_order) AS avg_days_since_prior_order
FROM orders
GROUP BY user_id;
