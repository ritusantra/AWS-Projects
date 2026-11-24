import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import *

# Initialize Glue Job
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'INPUT_PATH_C',
    'INPUT_PATH_T',
    'OUTPUT_PATH'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get parameters
ip_uri_c = args['INPUT_PATH_C']
ip_uri_t = args['INPUT_PATH_T']
op_uri = args['OUTPUT_PATH']


# Read Cleaned Data from S3
df_cust = spark.read.format('parquet').load(ip_uri_c)
df_txn = spark.read.format('parquet').load(ip_uri_t)


# Join Customer and Transaction Data
df_agg = df_cust.join(
    df_txn,
    on='customer_id',
    how='inner'
)


# Customer-Level Aggregation
df_agg = df_agg.groupBy(
    'customer_id',
    'name',
    'gender',
    'city',
    'status',
    'income_range',
    'risk_segment',
    'customer_age',
    'tenure'
).agg(
    countDistinct('account_id').alias('accounts'),
    count('txn_id').alias('transactions'),
    round(avg('txn_amount'), 2).alias('avg_txn_amt'),
    min('txn_date').alias('first_txn_dt'),
    max('txn_date').alias('last_txn_dt')
)


# Write Aggregated Data to S3
df_agg.write \
    .format('parquet') \
    .mode('append') \
    .option('path', op_uri) \
    .save()


# Commit Glue Job
job.commit()