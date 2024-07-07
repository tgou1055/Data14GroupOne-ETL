## hello world
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# important! using python min, max won't work
from pyspark.sql.functions import col, min, max, sum, avg, count, countDistinct, row_number
from pyspark.sql.window import Window

# https://spark.apache.org/docs/latest/sql-ref-datatypes.html
from pyspark.sql.types import StructType, StructField, BooleanType, ByteType, ShortType, IntegerType, StringType, FloatType, DoubleType
aisles_schema = StructType([
    StructField("aisle_id", IntegerType(), True),
    StructField("aisle", StringType(), True)
])
aisles = spark.read.csv("s3://data14group1-staging/aisles/aisles.csv", header=True, schema=aisles_schema)
aisles.write.mode("overwrite").parquet("s3://data14group1-transformed/aisles/")
aisles = spark.read.parquet('s3://data14group1-staging/aisles')
aisles.printSchema()

departments_schema = StructType([
    StructField("department_id", IntegerType(), True),
    StructField("department", StringType(), True)
])
departments = spark.read.csv("s3://data14group1-staging/departments/departments.csv", header=True, schema=departments_schema)
departments.write.mode("overwrite").parquet("s3://data14group1-transformed/departments/")
departments = spark.read.parquet('s3://data14group1-staging/departments') # read as parquet
departments.printSchema()

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("aisle_id", IntegerType(), True),
    StructField("department_id", IntegerType(), True)
])
products = spark.read.csv("s3://data14group1-staging/products/products.csv", header=True, schema=products_schema)
products.write.mode("overwrite").parquet("s3://data14group1-transformed/products/")
products = spark.read.parquet('s3://data14group1-staging/products') # read as parquet
products.printSchema()

products_denorm = products\
                    .join(aisles, products.aisle_id==aisles.aisle_id, 'inner')\
                    .join(departments, products.department_id==departments.department_id, 'inner')\
                    .select(products.product_id,
                            products.product_name,
                            products.aisle_id,
                            aisles.aisle,
                            products.department_id,
                            departments.department
                           )
products_denorm.printSchema()
products_denorm.write.mode("overwrite").parquet("s3://data14group1-transformed/products/")

