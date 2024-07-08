from pyspark.sql import SparkSession
from pyspark.sql.types import *
import time

# remove when uploading to gluejob
spark = SparkSession.builder \
    .appName("data14group1-ETL") \
    .getOrCreate()

executor_memory = spark.conf.get("spark.executor.memory")
print(f"Executor Memory: {executor_memory}")
driver_memory = spark.conf.get("spark.driver.memory")
print(f"Driver Memory: {driver_memory}")

# define functions
source = "s3://data14group1-staging"
dest = "s3://data14group1-transformed"
schema = dict()


def readFromCSV(dfName, prefix=source):
    return spark.read.csv(f"{prefix}/{dfName}", header=True, schema=schema[dfName])


def saveAsParquet(df, dfName, prefix=dest):
    df.write.mode('overwrite').parquet(f"{prefix}/{dfName}")
    print(f"{dfName} #. of partitions: {df.rdd.getNumPartitions()}")
    print(f"saved as parquet to {prefix}/{dfName}\n")


def createTempView(dfName, prefix=dest, cache=False):
    df = spark.read.parquet(f"{prefix}/{dfName}")
    if cache:
        df.cache()
    df.createOrReplaceTempView(dfName)
    return df


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

saveAsParquet(readFromCSV("aisles"), "aisles")
saveAsParquet(readFromCSV("departments"), "departments")
saveAsParquet(readFromCSV("products"), "products")

createTempView("aisles")
createTempView("departments")
createTempView("products")

products_denorm = spark.sql("""
select product_id, product_name, p.aisle_id, aisle, p.department_id, department
from products p 
join aisles a on p.aisle_id=a.aisle_id
join departments d on p.department_id=d.department_id
""")

saveAsParquet(products_denorm, "products_denorm")
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
with Timer("save orders.csv to parquet"):
    saveAsParquet(readFromCSV("orders"), "orders")
with Timer("save order_products__prior.csv to parquet"):
    saveAsParquet(readFromCSV("order_products__prior"), "order_products__prior")
orders = createTempView("orders", cache=True)
createTempView("order_products__prior")
order_products_prior = spark.sql("""
select 
o.order_id,o.user_id,o.order_number,o.order_dow,o.order_hour_of_day,o.days_since_prior_order,
opp.product_id,opp.add_to_cart_order,opp.reordered
from
orders o join order_products__prior opp on o.order_id=opp.order_id
where o.eval_set='prior'
""")
with Timer("order_products_prior"):
    saveAsParquet(order_products_prior, "order_products_prior")
user_features_1 = spark.sql("""
select 
    user_id, 
    max(order_number) as max_order_number, 
    sum(days_since_prior_order) as sum_days_since_prior_order, 
    avg(days_since_prior_order) as avg_days_since_prior_order
from orders
group by user_id
""")
with Timer("user_features_1"):
    saveAsParquet(user_features_1, "user_features_1")
orders.unpersist()
order_products_prior = createTempView("order_products_prior", cache=True)
user_features_2 = spark.sql("""
SELECT
    user_id,
    COUNT(product_id) AS total_products_count,
    COUNT(DISTINCT product_id) AS total_distinct_products_count, 
    SUM(CASE WHEN reordered = 1 THEN 1 ELSE 0 END) * 1.0 / 
    SUM(CASE WHEN order_number > 1 THEN 1 ELSE 0 END) AS reorder_ratio
FROM order_products_prior
GROUP BY user_id
""")
with Timer("user_features_2"):
    saveAsParquet(user_features_2, "user_features_2")
up_features = spark.sql("""
SELECT
    user_id,
    product_id,
    COUNT(order_id) AS total_orders,
    MIN(order_number) AS min_order_number,
    MAX(order_number) AS max_order_number,
    AVG(add_to_cart_order) AS avg_add_to_cart_order
FROM order_products_prior
GROUP BY user_id, product_id
""")
with Timer("up_features"):
    saveAsParquet(up_features, "up_features")
prod_seq = spark.sql("""
SELECT 
    product_id,
    COUNT(product_id) AS total_products,
    SUM(reordered) AS total_reordered,
    SUM(CASE WHEN product_seq_time = 1 THEN 1 ELSE 0 END) AS product_seq_time_is_1,
    SUM(CASE WHEN product_seq_time = 2 THEN 1 ELSE 0 END) AS product_seq_time_is_2
FROM (
    SELECT
        product_id,
        reordered,
        ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY order_number ASC) AS product_seq_time
    FROM order_products_prior
) prod_seq
GROUP BY product_id
""")
with Timer("prod_seq"):
    saveAsParquet(prod_seq, "prod_seq")
