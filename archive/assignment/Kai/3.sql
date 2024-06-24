Create Table user_features_2 with (
	external_location = 's3://weikaibucket/features/user_features_2/',
	format = 'parquet'
) as
select user_id,
	count(product_id) as total_number_of_products,
	count(Distinct product_id) as total_number_of_distinct_products,
	--整数除法。如果结果是一个小于 1 的小数，那么结果会被截断为 0,所以要乘以1.0变浮点数
	sum(if(reordered=1,1,0)) * 1.0 / sum(if(order_number>1,order_number,null)) as reorder_ratio
from order_products_prior
group by user_id;
