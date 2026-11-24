import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Glue Job
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'INPUT_PATH_C',
    'OUTPUT_PATH_C',
    'INPUT_PATH_T',
    'OUTPUT_PATH_T'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get parameters
ip_uri_c = args['INPUT_PATH_C']
op_uri_c = args['OUTPUT_PATH_C']

ip_uri_t = args['INPUT_PATH_T']
op_uri_t = args['OUTPUT_PATH_T']


# Read data from S3
df_cust = spark.read.format('csv') \
    .option('header', True) \
    .option('inferSchema', True) \
    .load(ip_uri_c)

df_txn = spark.read.format('csv') \
    .option('header', True) \
    .option('inferSchema', True) \
    .load(ip_uri_t)


# Clean Customers Data
df_cust = df_cust.withColumns({
    'name': upper(col('name')),
    'email': lower(col('email')),
    'occupation': upper(col('occupation')),
    'city': upper(col('city')),
    'income_range': upper(col('income_range')),
    'risk_segment': upper(col('risk_segment')),
    'preferred_channel': regexp_replace(
        upper(col('preferred_channel')), 'ONLNE', 'ONLINE'
    ),
    'gender': when(col('gender') == 'M', 'MALE')
        .when(col('gender') == 'F', 'FEMALE')
        .otherwise('UNKNOWN'),
    'status': when(
        col('status').isNull() | (trim(col('status')) == ''),
        'INACTIVE'
    ).otherwise(col('status')),
    'phone_number': regexp_replace(
        trim(col('phone_number')), r"[^0-9]", ""
    ),
    'transformation_date': current_timestamp()
}).where(col('phone_number').rlike(r"^44\d{10}$"))


# Clean Transactions Data
df_txn = df_txn.withColumns({
    'account_type': upper(col('account_type')),
    'txn_channel': upper(col('txn_channel')),
    'txn_type': when(upper(col('txn_type')) == 'DEBITT', 'DEBIT')
        .when(upper(col('txn_type')) == 'CREDIIT', 'CREDIT')
        .otherwise(upper(col('txn_type'))),
    'transformation_date': current_timestamp()
})


# Business Metrics on Customer Data
df_cust = df_cust.withColumns({
    'customer_age': when(
        col('dob').isNotNull(),
        floor(months_between(current_date(), col('dob')) / 12)
    ).otherwise(None),

    'tenure': when(
        col('join_date').isNotNull(),
        datediff(current_date(), col('join_date'))
    ).otherwise(None),

    'invalid_dob': when(
        (col('dob') > current_date()) |
        (col('dob') < lit('1900-01-01')),
        lit('Y')
    ).otherwise(lit('N'))
})

# Business Metrics on Transactions Data
df_txn = df_txn.withColumns({
    'channel_type': when(
        col('txn_channel').isin('ATM', 'BRANCH'), 'PHYSICAL'
    ).otherwise('DIGITAL'),

    'txn_year': year(col('txn_date')),
    'txn_month': month(col('txn_date')),
    'txn_day': dayofmonth(col('txn_date'))
})




# Write Customers Data to S3
df_cust.write \
    .format('parquet') \
    .mode('append') \
    .option('path', op_uri_c) \
    .save()


# Write Transactions Data to S3
df_txn.write \
    .format('parquet') \
    .mode('append') \
    .option('path', op_uri_t) \
    .save()


# Commit Glue Job
job.commit()