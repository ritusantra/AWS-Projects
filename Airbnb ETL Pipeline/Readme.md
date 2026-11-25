# Airbnb Data Engineering & ETL Pipeline

Developed an end-to-end ETL pipeline to process raw Airbnb datasets into structured, analytics-ready data using AWS services. The project demonstrates data ingestion, transformation, storage, cataloging, and SQL-based analysis to support data-driven business insights.

## Architecture & Workflow

- **Data Ingestion:** Uploaded and stored raw Airbnb datasets in an Amazon S3 bucket.
- **Data Processing:** Processed, cleaned, and transformed raw datasets using Python and SQL within an Amazon SageMaker Notebook instance.
- **Data Storage:** Stored the cleaned and transformed datasets in Amazon S3 for centralized and scalable data storage.
- **Data Cataloging:** Configured an AWS Glue Crawler to automatically discover schemas and register processed datasets in the AWS Glue Data Catalog.
- **Data Analysis:** Integrated Amazon Athena to enable SQL-based querying and analysis of the processed Airbnb datasets.

## AWS Services & Technologies Used

- **Amazon S3:** Storage for raw and processed datasets.
- **Amazon SageMaker AI:** Python-based data processing and transformation.
- **AWS Glue Crawler:** Automated schema discovery and data cataloging.
- **AWS Glue Data Catalog:** Metadata management.
- **Amazon Athena:** SQL-based data querying and analysis.
- **Python & SQL:** Data cleaning, transformation, and analysis.

## Architecture Diagram

<img width="1310" height="605" alt="Airbnb ETL Pipeline Architecture" src="https://github.com/user-attachments/assets/85deab84-1434-418c-8b79-87bbc6c29904" />
