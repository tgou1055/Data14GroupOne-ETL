import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, min, max, sum, avg, count, countDistinct, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, BooleanType, ByteType, ShortType, IntegerType, StringType, FloatType, DoubleType

def main():
    # Create SparkContext and GlueContext
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    
    # Define schemas and read CSV files into dataframes
    aisles_schema = StructType([
        StructField("aisle_id", IntegerType(), True),
        StructField("aisle", StringType(), True)
    ])
    aisles = spark.read.csv("s3://data14group1-raw/aisles.csv", header=True, schema=aisles_schema)
    aisles.write.mode("overwrite").parquet("s3://data14group1-staging/aisles/")
    aisles = spark.read.parquet("s3://data14group1-staging/aisles/")
    aisles.printSchema()
    print(f"row count: {aisles.count()}")
    
    departments_schema = StructType([
        StructField("department_id", IntegerType(), True),
        StructField("department", StringType(), True)
    ])
    departments = spark.read.csv("s3://data14group1-raw/departments.csv", header=True, schema=departments_schema)
    departments.write.mode("overwrite").parquet("s3://data14group1-staging/departments/")
    departments = spark.read.parquet("s3://data14group1-staging/departments/")
    departments.printSchema()
    print(f"row count: {departments.count()}")
    
    products_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("aisle_id", IntegerType(), True),
        StructField("department_id", IntegerType(), True)
    ])
    products = spark.read.csv("s3://data14group1-raw/products.csv", header=True, schema=products_schema)
    products.write.mode("overwrite").parquet("s3://data14group1-staging/products/")
    products = spark.read.parquet("s3://data14group1-staging/products/")
    products.printSchema()
    print(f"row count: {products.count()}")
    
    products_denorm = products \
        .join(aisles, products.aisle_id == aisles.aisle_id, "inner") \
        .join(departments, products.department_id == departments.department_id, "inner") \
        .select(products.product_id, products.product_name, products.aisle_id, aisles.aisle, products.department_id, departments.department)
    products_denorm.printSchema()
    products_denorm.write.mode("overwrite").parquet("s3://data14group1-transformed/products/")
        
    orders_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("eval_set", StringType(), True),
        StructField("order_number", IntegerType(), True),
        StructField("order_dow", ByteType(), True),
        StructField("order_hour_of_day", ByteType(), True),
        StructField("days_since_prior_order", FloatType(), True)
    ])
    orders = spark.read.csv("s3://data14group1-raw/orders.csv", header=True, schema=orders_schema)
    orders.write.partitionBy("eval_set").mode("overwrite").parquet("s3://data14group1-staging/orders/")
    orders = spark.read.parquet("s3://data14group1-staging/orders")
    orders.printSchema()
    print(f"row count: {orders.count()}")
    orders.agg(min("order_number"), max("order_number")).show()
    orders.agg(min("days_since_prior_order"), max("days_since_prior_order")).show()
    
    orders_prior = orders.where(orders.eval_set == "prior").select(*[c for c in orders.columns if c != "eval_set"])
    print(f"row count: {orders_prior.count()}")
    orders_prior.write.mode("overwrite").parquet("s3://data14group1-transformed/orders_prior/")
    
    order_products_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("add_to_cart_order", IntegerType(), True),
        StructField("reordered", IntegerType(), True)
    ])
    order_products = spark.read.csv("s3://data14group1-raw/order_products__prior.csv.gz", header=True, schema=order_products_schema)
    order_products = order_products.withColumn("reordered", col("reordered").cast("boolean"))
    order_products.write.mode("overwrite").parquet("s3://data14group1-staging/order_products__prior/")
    order_products = spark.read.parquet("s3://data14group1-staging/order_products__prior/")
    order_products.printSchema()
    print(f"row count: {order_products.count()}")
    
    order_products_prior = orders_prior \
        .join(order_products, orders_prior.order_id == order_products.order_id, "inner") \
        .select(orders_prior.order_id, orders_prior.user_id, orders_prior.order_number, orders_prior.order_dow,
                orders_prior.order_hour_of_day, orders_prior.days_since_prior_order, order_products.product_id,
                order_products.add_to_cart_order, order_products.reordered)
    order_products_prior.write.mode("overwrite").parquet("s3://data14group1-transformed/order_products_prior/")
        
    user_features_1 = orders.groupBy("user_id").agg(max("order_number").alias("max_order_number"),
                                                    sum("days_since_prior_order").alias("sum_days_since_prior_order"),
                                                    avg("days_since_prior_order").alias("avg_days_since_prior_order"))
    user_features_1.orderBy("user_id").show(5)
    print(f"row count: {user_features_1.count()}")
    user_features_1.write.mode("overwrite").parquet("s3://data14group1-transformed/features/user_features_1/")
        
    user_features_2 = order_products_prior.groupBy("user_id").agg(count("product_id").alias("total_products"),
                                                                  countDistinct("product_id").alias("total_distinct_products"),
                                                                  (sum(col("reordered").cast("int")) / sum((col("order_number") > 1).cast("int"))).alias("reorder_ratio"))
    user_features_2.orderBy("user_id").show(5)
    print(f"row count: {user_features_2.count()}")
    user_features_2.write.mode("overwrite").parquet("s3://data14group1-transformed/features/user_features_2/")
        
    up_features = order_products_prior.groupBy("user_id", "product_id").agg(count("order_id").alias("total_orders"),
                                                                           min("order_number").alias("min_order_number"),
                                                                           max("order_number").alias("max_order_number"),
                                                                           avg("add_to_cart_order").alias("avg_add_to_cart_order"))
    up_features.orderBy("user_id", "product_id").show(5)
    print(f"row count: {up_features.count()}")
    up_features.write.mode("overwrite").parquet("s3://data14group1-transformed/features/up_features/")
            
    prod_seq = order_products_prior.withColumn("product_seq_time", 
                                               row_number().over(Window.partitionBy("user_id", "product_id").orderBy(col("order_number").asc()))
                                              ).select("product_id", "reordered", "product_seq_time")

    prd_features = prod_seq.groupBy("product_id").agg(count("product_id").alias("total_products"),
                                                      sum(col("reordered").cast("int")).alias("total_reordered"),
                                                      sum((col("product_seq_time") == 1).cast("int")).alias("product_seq_time_is_1"),
                                                      sum((col("product_seq_time") == 2).cast("int")).alias("product_seq_time_is_2"))
    prd_features.orderBy("product_id").show(5)
    print(f"row count: {prd_features.count()}")
    prd_features.write.mode("overwrite").parquet("s3://data14group1-transformed/features/prd_features/")        
    
    # Creating dynamic frames from existing Athena catalog
    up_features = glueContext.create_dynamic_frame_from_options(connection_type="parquet", connection_options={"paths": ["s3://data14group1-transformed/features/up_features/"]})
    prd_features = glueContext.create_dynamic_frame_from_options(connection_type="parquet", connection_options={"paths": ["s3://data14group1-transformed/features/prd_features/"]})
    user_features_1 = glueContext.create_dynamic_frame_from_options(connection_type="parquet", connection_options={"paths": ["s3://data14group1-transformed/features/user_features_1/"]})
    user_features_2 = glueContext.create_dynamic_frame_from_options(connection_type="parquet", connection_options={"paths": ["s3://data14group1-transformed/features/user_features_2/"]})
    
    # Join user features together
    users = Join.apply(user_features_1.rename_field("user_id", "user_id1"), user_features_2, "user_id1", "user_id").drop_fields(["user_id1"])
    
    # Join everything together
    df = Join.apply(Join.apply(up_features, users.rename_field("user_id", "user_id1"), "user_id", "user_id1").drop_fields(["user_id1"]),
                    prd_features.rename_field("product_id", "product_id1"), "product_id", "product_id1").drop_fields(["product_id1"])
    
    # Convert Glue dynamic dataframe to Spark dataframe
    # 把gluejob生成的数据放到data14group1-ml这个bucket
    df_spark = df.toDF()
    df_spark.repartition(1).write.mode("overwrite").format("csv").save("s3://data14group1-transformed/output", header=True)

if __name__ == "__main__":
    main()
