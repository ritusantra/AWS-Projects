import json
import boto3

def lambda_handler(event, context):
        
    client = boto3.client('glue')

    response = client.start_job_run(JobName='customer_analytics_cleaned')

    return {
        'statusCode': 200
    }
