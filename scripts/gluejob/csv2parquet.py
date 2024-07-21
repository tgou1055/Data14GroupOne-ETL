#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
import time
import os

# prod
spark = SparkSession.builder.appName("data14group1-csv2parquet").getOrCreate()
source_bucket = "s3://data14group1-tim-staging"   # change this to your local bucket name
dest_bucket = "s3://data14group1-tim-transformed" # change this to your local bucket name

# In[ ]:


print("csv2parquet spark session created.")
# define functions
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

# ## stop spark session

# In[ ]:


spark.stop()

# In[ ]: