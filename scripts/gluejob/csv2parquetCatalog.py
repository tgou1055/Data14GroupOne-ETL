#!/usr/bin/env python
# coding: utf-8

import os
import time
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.types import *

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

#source_bucket = "s3://data14group1-staging"
dest_bucket = "s3://data14group1-transformed"
source_database = "staging"
dest_database = "transformed"

# create a spark df from catalog database and table 
def createDataFrame(table_name, database=source_database):
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table_name)
    return dynamic_frame.toDF()

# create temp view from catalog database and table
def createTempView(table_name, database=dest_database):
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table_name)
    df = dynamic_frame.toDF()
    df.createOrReplaceTempView(table_name)

# save as parquet
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


# save aisles, departments and products as parquet
saveAsParquet(createDataFrame("aisles"), "aisles", prefix="intermediate")
saveAsParquet(createDataFrame("departments"), "departments", prefix="intermediate")
saveAsParquet(createDataFrame("products"), "products", prefix="intermediate")


# denorm products
createTempView("aisles")
createTempView("departments")
createTempView("products")

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

# save orders, order_products_prior to parquet
with Timer("save orders.csv to parquet"):
    saveAsParquet(createDataFrame("orders"), "orders", prefix="intermediate")
with Timer("save order_products__prior.csv to parquet"):
    saveAsParquet(createDataFrame("order_products__prior"), "order_products__prior", prefix="intermediate")
with Timer("save order_products__train.csv to parquet"):
    saveAsParquet(createDataFrame("order_products__train"), "order_products__train", prefix="intermediate")

# stop spark session
spark.stop()
job.commit()
