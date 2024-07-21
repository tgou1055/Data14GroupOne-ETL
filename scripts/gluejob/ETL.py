#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import time
import os

# prod
spark = SparkSession.builder.appName("data14group1-csv2parquet").getOrCreate()
source_bucket = "s3://data14group1-tim-transformed" # change this to your local bucket name
dest_bucket = "s3://data14group1-tim-ml" # change this to your local bucket name

def createTempView(dfName, bucket, prefix=None):
    df = spark.read.parquet(os.path.join(*[p for p in [bucket, prefix, dfName] if p]))
    df.createOrReplaceTempView(dfName)

def saveAsParquet(df, dfName, bucket=dest_bucket, prefix="data"):
    path = os.path.join(*[p for p in [bucket, prefix, dfName] if p])
    df.write.mode('overwrite').parquet(path)
    print(f"{dfName} #. of partitions: {df.rdd.getNumPartitions()}")
    print(f"saved as parquet to {path}\n")


class Timer:
    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.end_time = time.time()
        self.execution_time = self.end_time - self.start_time
        print(f"{self.name} execution time: {self.execution_time:.3f} seconds")


createTempView("orders", source_bucket, prefix="intermediate")
createTempView("order_products__prior", source_bucket, prefix="intermediate")
createTempView("order_products__train", source_bucket, prefix="intermediate")
createTempView("products_denorm", source_bucket, prefix="intermediate")

order_products_prior = spark.sql("""
SELECT 
    *
FROM
    orders
JOIN 
    order_products__prior USING (order_id)
WHERE
    eval_set = 'prior'
""").cache()
order_products_prior.createOrReplaceTempView("order_products_prior")

order_products_train = spark.sql("""
SELECT 
    *
FROM
    orders
JOIN 
    order_products__train USING (order_id)
WHERE
    eval_set = 'train'
""")
order_products_train.createOrReplaceTempView("order_products_train")

# Q2 user order interaction
user_features_1 = spark.sql("""
SELECT
    user_id, 
    MAX(order_number) AS user_orders, 
    SUM(days_since_prior_order) AS user_sum_days_since_prior, 
    AVG(days_since_prior_order) AS user_mean_days_since_prior
FROM orders
WHERE eval_set='prior'
GROUP BY user_id
""")
user_features_1.createOrReplaceTempView("user_features_1")

# Q3 user product interaction
user_features_2 = spark.sql("""
SELECT
    user_id,
    COUNT(product_id) AS user_total_products,
    COUNT(DISTINCT product_id) AS user_distinct_products, 
    SUM(CASE WHEN reordered = 1 THEN 1 ELSE 0 END) * 1.0 / 
    SUM(CASE WHEN order_number > 1 THEN 1 ELSE 0 END) AS user_reorder_ratio
FROM order_products_prior
GROUP BY user_id
""")
user_features_2.createOrReplaceTempView("user_features_2")

# Q4
up_features = spark.sql("""
SELECT
    user_id,
    product_id,
    COUNT(order_id) AS up_orders,
    MIN(order_number) AS up_first_order,
    MAX(order_number) AS up_last_order,
    AVG(add_to_cart_order) AS up_average_cart_position
FROM order_products_prior
GROUP BY user_id, product_id
""")
up_features.createOrReplaceTempView("up_features")

# Q5
prod_features = spark.sql("""
SELECT 
    product_id,
    prod_orders,
    prod_second_orders * 1.0 / prod_first_orders AS prod_reorder_probability,
    1 + prod_reorders * 1.0 / prod_first_orders AS prod_reorder_times,
    prod_reorders * 1.0 / prod_orders AS prod_reorder_ratio
FROM (
    SELECT 
        product_id,
        COUNT(product_id) AS prod_orders,
        SUM(reordered) AS prod_reorders,
        SUM(CASE WHEN product_seq_time = 1 THEN 1 ELSE 0 END) AS prod_first_orders,
        SUM(CASE WHEN product_seq_time = 2 THEN 1 ELSE 0 END) AS prod_second_orders
    FROM (
        SELECT
            product_id,
            reordered,
            RANK() OVER (PARTITION BY user_id, product_id ORDER BY order_number ASC) AS product_seq_time
        FROM order_products_prior
    )
    GROUP BY product_id
)
""")
prod_features.createOrReplaceTempView("prod_features")

# Join features together and add a few more
output = spark.sql("""
SELECT
    *,
    user_total_products * 1.0 / user_orders AS user_average_basket,
    up_orders * 1.0 / user_orders AS up_order_rate,
    user_orders - up_last_order AS up_orders_since_last_order,
    up_orders * 1.0 / (user_orders - up_first_order + 1) AS up_order_rate_since_first_order
FROM (
    SELECT *
    FROM user_features_1
    JOIN user_features_2 USING (user_id)
)
JOIN up_features USING (user_id)
JOIN prod_features USING (product_id)
""").cache()
output.createOrReplaceTempView("output")

order_products_prior.unpersist()

# Create train and test data
trainval = spark.sql("""
SELECT 
    *
FROM 
    output
JOIN 
    (
    SELECT DISTINCT user_id 
    FROM orders 
    WHERE eval_set = 'train'
    ) 
USING (user_id)
LEFT JOIN 
    (
    SELECT
        user_id,
        product_id,
        reordered
    FROM order_products_train
    )
USING (user_id, product_id);

""").drop("user_id", "product_id").fillna({'reordered': 0})
columns = trainval.columns
trainval = trainval.select(*[columns[-1], *columns[:-1]])  # put last column (reordered) as first
with Timer("trainval"):
    saveAsParquet(trainval, "trainval")

test = spark.sql("""
SELECT *
FROM (
    SELECT user_id
    FROM orders
    WHERE eval_set = 'test'
)
JOIN output USING (user_id)
JOIN (
    SELECT
        product_id,
        product_name, 
        aisle, 
        department 
    FROM products_denorm
) USING (product_id)
""")
with Timer("test"):
    saveAsParquet(test, "test")

output.unpersist()

# Commit job
spark.stop()
