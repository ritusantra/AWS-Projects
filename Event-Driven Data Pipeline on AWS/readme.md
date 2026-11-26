# Design and Implementation of an Automated Event-Driven ETL Pipeline for Scalable Data Ingestion, Transformation, and Analytics Using AWS

## Project Overview

Designed and implemented an automated, serverless ETL data pipeline using AWS services to ingest, clean, transform, and aggregate CSV files stored in Amazon S3. The pipeline leverages AWS Lambda, AWS Glue, AWS Glue Crawlers, and Amazon Athena to build a structured data lake for analytics and querying.

## Architecture & Workflow

### 1. Data Ingestion
- CSV files are uploaded to the landing layer of an Amazon S3 bucket, serving as the initial data ingestion point for raw datasets.

### 2. Automated Data Cleaning
- An S3 event triggers an AWS Lambda function when new files are uploaded.
- The Lambda function initiates an AWS Glue ETL job to clean and transform the raw data.
- The processed data is stored in a separate cleaned S3 bucket.

### 3. Data Aggregation
- A AWS Glue ETL job processes the cleaned data.
- Performs data aggregations and transformations.
- Stores the final aggregated datasets in a dedicated aggregation S3 bucket.

### 4. Data Cataloging
- AWS Glue Crawlers discover schemas and register tables for both cleaned and aggregated datasets.
- Metadata is maintained in the AWS Glue Data Catalog.

### 5. Data Analytics
- Amazon Athena is used to query cleaned and aggregated datasets directly using SQL.
- Enables data analysis without requiring additional data infrastructure.

## AWS Services Used

| AWS Service | Purpose |
|---|---|
| Amazon S3 | Data storage across landing, cleaned, and aggregated layers |
| AWS Lambda | Event-driven automation to trigger ETL workflows |
| AWS Glue | Data cleaning, transformation, and aggregation |
| AWS Glue Crawlers | Schema discovery and table registration |
| AWS Glue Data Catalog | Centralized metadata management |
| Amazon Athena | SQL-based querying and data analysis |

## Key Features

- Automated, event-driven ETL processing.
- Multi-layer data lake architecture.
- Separation of raw, cleaned, and aggregated datasets.
- Centralized metadata management using AWS Glue Data Catalog.
- Serverless data processing and SQL-based analytics through Amazon Athena.


## AWS Views
<img width="1913" height="658" alt="image" src="https://github.com/user-attachments/assets/5d32e786-d3a1-4d6e-898e-08816111cac6" />
<img width="1239" height="637" alt="image" src="https://github.com/user-attachments/assets/2275eca4-510f-4f7e-9ec0-1e8d442bbc6c" />
<img width="1549" height="666" alt="image" src="https://github.com/user-attachments/assets/8255330c-5558-4180-bf70-820602617eb1" />
<img width="1567" height="718" alt="image" src="https://github.com/user-attachments/assets/32050e23-9e01-430b-9e0e-d35cf4528703" />

