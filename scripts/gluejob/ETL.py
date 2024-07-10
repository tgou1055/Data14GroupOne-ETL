#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
import time
import os

# remove when uploading to gluejob
spark = SparkSession.builder.appName("data14group1-ETL").getOrCreate()

executor_memory = spark.conf.get("spark.executor.memory")
print(f"Executor Memory: {executor_memory}")
driver_memory = spark.conf.get("spark.driver.memory")
print(f"Driver Memory: {driver_memory}")

# In[ ]:


# define functions
source_bucket = "data14group1-staging"
dest_bucket = "data14group1-transformed"
ml_bucket = "data14group1-ml"
schema = dict()


def readFromCSV(dfName, bucket=source_bucket):
    return spark.read.csv(os.path.join(bucket, dfName),
                          header=True,
                          schema=schema[dfName])


def createTempView(dfName, bucket=dest_bucket, prefix=None):
    df = spark.read.parquet(os.path.join(*[p for p in [bucket, prefix, dfName] if p]))
    df.createOrReplaceTempView(dfName)


def saveAsParquet(df, dfName, bucket=dest_bucket, prefix=None):
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


# ## save aisles, departments and products as parquet

# In[ ]:


schema["aisles"] = StructType([
    StructField("aisle_id", IntegerType(), True),
    StructField("aisle", StringType(), True)
])
schema["departments"] = StructType([
    StructField("department_id", IntegerType(), True),
    StructField("department", StringType(), True)
])
schema["products"] = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("aisle_id", IntegerType(), True),
    StructField("department_id", IntegerType(), True)
])

saveAsParquet(readFromCSV("aisles"), "aisles", prefix="intermediate")
saveAsParquet(readFromCSV("departments"), "departments", prefix="intermediate")
saveAsParquet(readFromCSV("products"), "products", prefix="intermediate")

# ## denorm products

# In[ ]:


createTempView("aisles", prefix="intermediate")
createTempView("departments", prefix="intermediate")
createTempView("products", prefix="intermediate")

products_denorm = spark.sql("""
SELECT
    *
FROM
    products
JOIN
    aisles USING (aisle_id)
JOIN
    departments USING (department_id)
""")

saveAsParquet(products_denorm, "products_denorm", prefix="intermediate")

# ## save orders, order_products_prior to parquet

# In[ ]:


schema["orders"] = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("eval_set", StringType(), True),
    StructField("order_number", IntegerType(), True),
    StructField("order_dow", ByteType(), True),
    StructField("order_hour_of_day", ByteType(), True),
    StructField("days_since_prior_order", FloatType(), True)
])
schema["order_products__prior"] = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("add_to_cart_order", IntegerType(), True),
    StructField("reordered", IntegerType(), True)
])
schema["order_products__train"] = schema["order_products__prior"]
with Timer("save orders.csv to parquet"):
    saveAsParquet(readFromCSV("orders"), "orders", prefix="intermediate")
with Timer("save order_products__prior.csv to parquet"):
    saveAsParquet(readFromCSV("order_products__prior"), "order_products__prior", prefix="intermediate")
with Timer("save order_products__train.csv to parquet"):
    saveAsParquet(readFromCSV("order_products__train"), "order_products__train", prefix="intermediate")

# In[ ]:


createTempView("orders", prefix="intermediate")
createTempView("order_products__prior", prefix="intermediate")
createTempView("order_products__train", prefix="intermediate")

# In[ ]:


order_products_prior = spark.sql("""
SELECT 
    *
FROM
    orders
JOIN 
    order_products__prior USING (order_id)
WHERE
    eval_set = 'prior'
""")

with Timer("order_products_prior"):
    saveAsParquet(order_products_prior, "order_products_prior", prefix="intermediate")

# In[ ]:


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
with Timer("order_products_train"):
    saveAsParquet(order_products_train, "order_products_train", prefix="intermediate")

# ## Q2 user order interaction

# In[ ]:


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
with Timer("user_features_1"):
    saveAsParquet(user_features_1, "user_features_1", prefix="features")

# ## Q3 user product interaction

# In[ ]:


createTempView("order_products_prior", prefix="intermediate")

# In[ ]:


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
with Timer("user_features_2"):
    saveAsParquet(user_features_2, "user_features_2", prefix="features")

# ## Q4

# In[ ]:


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
with Timer("up_features"):
    saveAsParquet(up_features, "up_features", prefix="features")

# ## Q5

# In[ ]:


prod_features = spark.sql("""
SELECT 
    product_id,
    prod_orders,
    prod_reorder_probability,
    prod_reorder_times,
    prod_reorder_ratio
FROM (
    SELECT 
        product_id,
        COUNT(product_id) AS prod_orders,
        SUM(reordered) AS prod_reorders,
        SUM(CASE WHEN product_seq_time = 1 THEN 1 ELSE 0 END) AS prod_first_orders,
        SUM(CASE WHEN product_seq_time = 2 THEN 1 ELSE 0 END) AS prod_second_orders,
        prod_second_orders * 1.0 / prod_first_orders AS prod_reorder_probability,
        1 + prod_reorders * 1.0 / prod_first_orders AS prod_reorder_times,
        prod_reorders / prod_orders AS prod_reorder_ratio
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
with Timer("prod_features"):
    saveAsParquet(prod_features, "prod_features", prefix="features")

# ## Join features together and add a few more

# In[ ]:


createTempView("user_features_1", prefix="features")
createTempView("user_features_2", prefix="features")
createTempView("up_features", prefix="features")
createTempView("prod_features", prefix="features")

# In[ ]:


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
""")
with Timer("output"):
    saveAsParquet(output, "output", prefix="features")

# In[ ]:


createTempView("output", prefix="features")
createTempView("order_products_train", prefix="intermediate")

# ## create train and test data

# In[ ]:


train = spark.sql("""
SELECT 
    *
FROM 
    output
JOIN 
    (SELECT
        user_id,
        product_id,
        reordered
    FROM order_products_train
    )
USING 
    (user_id, product_id)
""").drop("user_id", "product_id")
columns = train.columns
train = train.select(*[columns[-1], *columns[:-1]])  # put last column (reordered) as first
with Timer("train"):
    saveAsParquet(train, "train", bucket=ml_bucket)

# In[ ]:


createTempView("products_denorm", prefix="intermediate")

# In[ ]:


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
    saveAsParquet(test, "test", bucket=ml_bucket)

# In[ ]:


spark.stop()

# In[ ]:



