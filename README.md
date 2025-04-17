### Amazon Best Sellers Data Pipeline
<br><br>
**Project Overview**
This project implements an automated data pipeline that extracts, transforms, and loads Amazon Best Sellers data from the RapidAPI Amazon Real-Time Data API into an AWS S3 data lake. The pipeline is orchestrated using Apache Airflow and runs on a daily schedule.

<br>
<b>Data Source</b>: RapidAPI Amazon Real-Time Data API<br>
<b>Orchestration</b>: Apache Airflow<br>
<b>Storage</b>: AWS S3<br>
<b>Data Processing</b>: Python with Pandas and AWS Wrangler<br>
<br><br>
<b>Key Features</b>
<br>
1. Daily extraction of Amazon Best Sellers data for software products<br>
2. Data validation and API availability checks<br>
3. Data transformation and cleaning<br>
4. Parquet file storage in S3 data lake<br>

<br><br>
**DAG Structure**
<br>
![image](https://github.com/user-attachments/assets/3ce34e33-1c57-45d5-bca8-a4402aa67879)

<br>
The pipeline consists of four main tasks:<br>

**API Availability Check**: Validates the API endpoint is accessible<br>
**Data Extraction**: Pulls raw data from the RapidAPI endpoint <br>
**Data Transformation**: Cleans and structures the data <br>
**S3 Storage**: Writes transformed data to S3 in Parquet format <br>
<br><br>
**Prerequisites**
<br>
Apache Airflow installation<br>
AWS credentials with S3 write access<br>
RapidAPI account with access to Amazon Real-Time Data API<br>
Python packages: pandas, awswrangler, boto3, requests<br>

