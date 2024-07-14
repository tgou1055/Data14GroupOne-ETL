import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import time

# Initialize Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define source and destination databases
source_database = "transformed"
ml_database = "ml"

# Define utility functions
def createTempView(database, table_name):
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table_name)
    df = dynamic_frame.toDF()
    df.createOrReplaceTempView(table_name)

class Timer:
    def __init__(self, name):
        self.name = name
        
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.end_time = self.start_time
        self.execution_time = self.end_time - self.start_time
        print(f"{self.name} execution time: {self.execution_time:.3f} seconds")

# Create temporary views from Glue catalog tables
createTempView(source_database, "orders")
createTempView(source_database, "order_products__prior")
createTempView(source_database, "order_products__train")
createTempView(source_database, "products_denorm")

# SQL queries to process data
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

# User order interaction features
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

# User product interaction features
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

# Product features
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

# Join features together and add additional features
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

# Create train and test data
order_products_prior.unpersist()

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
USING (user_id, product_id)
""").drop("user_id", "product_id").fillna({'reordered': 0})
columns = trainval.columns
trainval = trainval.select(*[columns[-1], *columns[:-1]]) # put last column (reordered) as first

# Just to time the trainval computation
with Timer("trainval"):
    trainval_dynamic_frame = DynamicFrame.fromDF(trainval, glueContext, "trainval")

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

# Just to time the test computation
with Timer("test"):
    test_dynamic_frame = DynamicFrame.fromDF(test, glueContext, "test")

output.unpersist()