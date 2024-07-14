import os
import sys
import time

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize context and job
args = getResolvedOptions(sys.argv, ["data14group1-csv2parquet"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["data14group1-csv2parquet"], args)

dest_bucket = "s3://data14group1-transformed"
prefix = "intermediate"


# define functions
def readFromCatalog(database, table, createTempView=False):
    data = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table)
    df = data.toDF()
    if createTempView:
        df.createOrReplaceTempView(table)
    return df


def saveAsParquet(df, dfName, bucket=dest_bucket, prefix=prefix):
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


# Save data from csv to parquet
saveAsParquet(readFromCatalog("staging", "aisles"), "aisles")
saveAsParquet(readFromCatalog("staging", "departments"), "departments")
saveAsParquet(readFromCatalog("staging", "products"), "products")
with Timer("save orders.csv to parquet"):
    saveAsParquet(readFromCatalog("staging", "orders"), "orders")
with Timer("save order_products__prior.csv to parquet"):
    saveAsParquet(readFromCatalog("staging", "order_products__prior"), "order_products__prior")
with Timer("save order_products__train.csv to parquet"):
    saveAsParquet(readFromCatalog("staging", "order_products__train"), "order_products__train")

# Denorm products
readFromCatalog("transformed", "order_products__train", createTempView=True)
readFromCatalog("transformed", "departments", createTempView=True)
readFromCatalog("transformed", "products", createTempView=True)

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
saveAsParquet(products_denorm, "products_denorm")

# Commit job
job.commit()
