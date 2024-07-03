Create Table up_features with (
	external_location = 's3://weikaibucket/features/up_features/',
	format = 'parquet'
) as
select user_id,
	product_id,
	count(order_id) as total_number_of_orders,
	min(order_number) as minimum_order_number,
	max(order_number) as max_order_number,
	avg(add_to_cart_order) as avg_add_to_cart_order
from order_products_prior
group by user_id,
	product_id;
