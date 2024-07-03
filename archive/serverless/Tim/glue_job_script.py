import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

from pyspark.sql.types import StructType, StructField, BooleanType, ByteType, ShortType, IntegerType, StringType, FloatType, DoubleType
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


# Database and table names
source_database = "cfn_imba_database_csv"
source_table = "aisles"
target_path = "s3://tgou1055-imba-sls-event/data_parquet/aisles/"

# Read data from the catalog
#dyf = glueContext.create_dynamic_frame.from_catalog(database = source_database, table_name = source_table)
#dyf.printSchema()
#df = dyf.toDF()


aisles_schema = StructType([
    StructField("aisle_id", IntegerType(), True),
    StructField("aisle", StringType(), True)
])
aisles_df = spark.read.csv("s3://tgou1055-imba-sls-event/data_csv/aisles/aisles.csv", header=True, schema=aisles_schema)
aisles_df.write.mode("overwrite").parquet(target_path)

# Write data to s3 local with parquet

spark.stop()
job.commit()