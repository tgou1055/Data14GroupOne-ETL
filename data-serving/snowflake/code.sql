-- Create database
create database data14GroupOne;
use database data14GroupOne;

-- create integration
CREATE STORAGE INTEGRATION s3_integration_orders 
    TYPE = EXTERNAL_STAGE 
    STORAGE_PROVIDER = 'S3' 
    ENABLED = TRUE 
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::730335478019:role/snowflake-access-s3-orders-role' 
    STORAGE_ALLOWED_LOCATIONS = ('*');

-- describe integration, get snowflake IAM user ARN and external ID
DESC INTEGRATION s3_integration_orders;

-- create stage using s3 integration
CREATE OR REPLACE STAGE s3_stage_orders 
    STORAGE_INTEGRATION = s3_integration_orders 
    URL = 's3://sam-raw/orders';

-- define file format for csv
CREATE OR REPLACE FILE FORMAT csv_format 
TYPE = 'CSV' 
FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
SKIP_HEADER = 1;
-- create external table
CREATE OR REPLACE EXTERNAL TABLE orders (
    order_id NUMBER AS (VALUE:c1::NUMBER),
    user_id NUMBER AS (VALUE:c2::NUMBER),
    eval_set varchar(10) AS (VALUE:c3::varchar),
    order_number number AS (VALUE:c4::NUMBER),
    order_dow NUMBER(1, 0) AS (VALUE:c5::NUMBER),
    order_hour_of_day number AS (VALUE:c6::NUMBER),
    days_since_prior_order number AS (VALUE:c7::NUMBER)
) WITH 
LOCATION = @s3_stage_orders 
FILE_FORMAT = (FORMAT_NAME = csv_format) 
AUTO_REFRESH = TRUE 
PATTERN = '.*orders.csv';

-- query
select * from orders limit 10;

-- query
select
    user_id,
    max(order_number) as "max order number",
    sum(days_since_prior_order) as "sum of days since prior order",
    avg(days_since_prior_order) as "avg of days since prior order"
from
    orders
group by
    user_id;

