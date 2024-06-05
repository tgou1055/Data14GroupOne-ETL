Create Table prd_features with (
	external_location = 's3://weikaibucket/features/prd_features/',
	format = 'parquet'
) as
select product_id,
	count(product_id) as count_product,
	sum(reordered) as sum_of_reordered,
	count(
		if (product_seq_table.product_seq_time = 1, 1, null)
	) as seq_time1,
	count(
		if (product_seq_table.product_seq_time = 2, 1, null)
	) as seq_time2
from (
		select user_id,
			product_id,
			reordered,
			row_number() over (
				partition by user_id,
				product_id
				order by days_since_prior_order Desc
			) as product_seq_time
		from order_products_prior
	) as product_seq_table
group by product_id;
