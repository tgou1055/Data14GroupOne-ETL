
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from pyspark.sql.functions import lit, col, isnull, when
import pandas as pd
'''
 We load the data.parquet of the joined data set of features of SQL assignment 1
'''
# File path to the input PARQUET file
parquet_file_path = "s3://tgou1055-imba-sls-deployment/features/features_aggregation/" # modify to s3 bucket path

# Read the PARQUET file with schema inference
# This is the data df of the features aggregation from SQL assignment 1
data_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .parquet(parquet_file_path)

# File path to the input PARQUET file
parquet_file_path = "s3://tgou1055-imba-sls-event/data_parquet/order_products/" # modify to s3 bucket path

# Read the PARQUET file with schema inference
order_products_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .parquet(parquet_file_path)

# File path to the input PARQUET file
parquet_file_path = "s3://tgou1055-imba-sls-event/data_parquet/orders/" # modify to s3 bucket path

# Read the PARQUET file with schema inference
orders_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .parquet(parquet_file_path)

print("load data complete")
# rename column order_id to order_id_1
filter_orders_df = orders_df.filter(orders_df['eval_set']=='train')[['user_id','order_id']].withColumnRenamed('order_id', 'order_id_1')

# add order_id to order_products_train
order_products_train_df = order_products_df.join(filter_orders_df, \
                                                       order_products_df['order_id'] == filter_orders_df['order_id_1'], 'inner')
order_products_train_new_df = order_products_train_df.drop('order_id_1')
#order_products_train_new_df.show(2)

print('transformation complete')
# attach eval_set to data
'''
In the following transformation, we add 'eval_set' infomation to the features 'data' we imported.
Notice that for each 'user_id' with 'eval_set = train or test', there is only one 'order_id'
( This is reasonable, because we try to train the model with the lastest orders of users,
  with the features deduced with 'eval_set = prior'
)
'''
filtered_orders_2_df = orders_df[orders_df['eval_set'] != 'prior'][['user_id','eval_set']].withColumnRenamed('user_id', 'user_id_1')
data_df = data_df.join(filtered_orders_2_df, data_df['user_id']==filtered_orders_2_df['user_id_1'], 'inner').drop('user_id_1')
print("transformation complete")
# attach target variable: reordered
'''
The data now contains all the prepared features for those orders of a user that has 
    eval_set = 'train' or 'test' 

    We use the left join to join 'data' and order_products_train
'''
order_products_train_new_df = order_products_train_new_df[['user_id', 'product_id', 'reordered']]
order_products_train_new_df = order_products_train_new_df.withColumnRenamed('user_id','user_id_1').withColumnRenamed('product_id','product_id_1')

data_new_df = data_df.join(order_products_train_new_df, \
        (data_df['user_id'] == order_products_train_new_df['user_id_1'])&(data_df['product_id'] == order_products_train_new_df['product_id_1']), \
        'left').drop('user_id_1').drop('product_id_1')

print('transformation complete')
data_new_df.printSchema()
data_new_df = data_new_df.withColumn('prod_reorder_probability', col('seq_is_two')/col('seq_is_one'))
data_new_df = data_new_df.withColumn('prod_reorder_times', 1 + col('sum_reordered')/col('seq_is_one'))
data_new_df = data_new_df.withColumn('prod_reorder_ratio', col('sum_reordered')/col('num_product_ordered'))
data_new_df = data_new_df.drop('sum_reordered', 'seq_is_one', 'seq_is_two')
data_new_df = data_new_df.withColumn('user_average_basket', col('num_product_bought')/col('max_order_number'))
data_new_df = data_new_df.withColumn('up_order_rate', col('number_of_orders')/col('max_order_number'))
data_new_df = data_new_df.withColumn('up_orders_since_last_order', col('max_order_number') - col('prod_max_order_num'))
data_new_df = data_new_df.withColumn('up_order_rate_since_first_order', col('number_of_orders') / (col('max_order_number') - col('prod_min_order_num')+1))
print("new features added")
# store the data in parquet file
data_new_df = data_new_df.coalesce(1)
output_path = "s3://tgou1055-imba-sls-deployment/model-training/data-features-advanced"
data_new_df.write.mode('overwrite').parquet(output_path)
print("data saving complete")
# split into training and test set, test set does not have target variable
train_df = data_new_df.filter(data_new_df['eval_set'] == 'train')
test_df = data_new_df.filter(data_new_df['eval_set'] == 'test')
print("data split into train and test")
# id field won't be used in model, thus make a backup of them and remove from dataframe
test_id_df = test_df[['product_id','user_id', 'eval_set','reordered']]
test_df = test_df.drop('product_id','user_id', 'eval_set', 'reordered')
print("test data prepared")
# convert target variable to 1/0 for training dataframe
from pyspark.sql.types import IntegerType, StringType
train_df = train_df.fillna({'reordered': 0})
train_df = train_df.withColumn('reordered', train_df['reordered'].cast(IntegerType()))
print("null values of train_df['reordered'] are filled by 0")
# drop id columns as they won't be used in model
train_df = train_df.drop('eval_set', 'user_id', 'product_id')
df = train_df
print("id columns in train_df are dropped")
# Split the df DataFrame into training (80%) and validation (20%) sets
seed = 41
train_df, validation_df = df.randomSplit([0.8, 0.2], seed=seed)
print("train (80%) and validation (20%) sets are randomly selected")
# This is the target variable dataframe
train_y = train_df[['reordered']]
validation_y = validation_df[['reordered']]
# This is the dataframe without target variable and contains all the features
train_X = train_df.drop('reordered')
validation_X = validation_df.drop('reordered')
print("target y and input X values of train and validations sets are prepared")
# Reorder columns, and put target y column to the first column
columns = ['reordered'] + [col for col in df.columns if col != 'reordered']
train_new_df = train_df.select(columns)
validation_new_df = validation_df.select(columns)
print("model training and validation sets are ready!")
#train_new_df.printSchema()
# The built-in SageMaker XGBoost algorithm primarily expects input data in CSV or libSVM format.
train = train_new_df.coalesce(1)
path_train = "s3://tgou1055-imba-sls-deployment/model-training/train/" # modify to s3 bucket path
train.write.mode('overwrite').csv(path_train, header=False)
print('train file saving complete')
# The built-in SageMaker XGBoost algorithm primarily expects input data in CSV or libSVM format.
validation= validation_new_df.coalesce(1)
path_validation = "s3://tgou1055-imba-sls-deployment/model-training/validation/" # modify to s3 bucket path
validation.write.mode('overwrite').csv(path_validation, header=False)
print('validation file saving complete')
# The built-in SageMaker XGBoost algorithm primarily expects input data in CSV or libSVM format.
test_id = test_id_df.coalesce(1)
test = test_df.coalesce(1)
path_test_id = "s3://tgou1055-imba-sls-deployment/model-training/test_id/"
path_test = "s3://tgou1055-imba-sls-deployment/model-training/test/"
test_id.write.mode('overwrite').csv(path_test_id, header=False)
test.write.mode('overwrite').csv(path_test, header=False)

print('test_id and test files saving complete')

spark.stop()
job.commit()