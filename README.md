# Lake to warehouse batch pipeline for retail enterprise data

#### Run this project
```
docker compose up -d
./setup.sh

# Spark UI: localhost:8080
# Airflow Webserver: localhost:8081
# Warehouse (PostgreSQL): localhost:5432
```
## Overview
In this system, a DVD rental retail enterprise wants to load large amounts of operational data into a data warehouse (dimensional model) from data stored in various different parts of their system on a daily basis, for data analysis/business intelligence. The data pipeline has to be **idempotent, scalable, low cloud-cost, and automated** with Type 2 Slowly Changing Dimension so as to not lose data in the past as dimensions are updated. Tech used is Spark, S3, PostgreSQL, Airflow, Docker compose.

Components:
- Data lake (assume the data from various sources are consolidated here before pipeline begins)
- Data processor 
- Data Warehouse (stores all historical data)
- Data Mart (Analysts use this. **It is a copy of the data warehouse, or subset of it, that is written to only when pipeline End state is successful**)

## The data
This diagram shows the different data sources, the transformations that need to take place, and the dimensional schema of the data warehouse. Assume that each table represents a different source stored in different locations. The red circle shows data used to form the Fact table, and the green circles show data used to form dimensional tables.

![Data](/images/Data%20transformation.png "Pipeline")

There sources are categorised into two types of data that are treated differently because of the scale at which new records arrive. Dimensional data and Transactional data.
Dimensional data (customer, product, outlet) has only a small number of additions compared to transactional data (sales, payments, inventory).

Regular operations barring pipeline errors (such as incorrect data being added that needs to be changed):
* Dimensional: Add/update rows
* Fact: Add rows

### Dag, and daily pipeline timeline
The pipeline runs on an automated schedule every day after the storage of the previous day's data is completed so as to make yesterday's data immediately available for analysis the next working day (7am). This is the schedule of jobs:
- 00:00 Logical end of the previous day's data. All writes after this are recorded with a different date
- 00:10 Data from various sources in the system are copied into the data lake to prepare for pipeline start
- 03:00 All OLTP writes are complete (assuming that even large numbers of data collected are complete by 3am because system is scalable)
- 03:10 Pipeline started and ETL process is underway
- 06:00 Pipeline is completed and status of the Airflow tasks are logged. Pipeline is either success or fail. Data Mart is updated only if entire pipeline is a success.

### DAG on Airflow
This diagram shows the DAG of the tasks in this pipeline and how airflow orchestrates them. There are data validation tasks for the day's data to ensure that data is clean and available for transformation. This should be extended for dimensional data as well, not just transactional data. ETL tasks (cashing, deduplication, transformation, load) which operate on dimensional table data BEFORE fact table data so as to meet referential integrity constraints in the DWH. The only variable in this batch pipeline is the date of the previous day (batch_date or insert_date), this is for sink overwritability (fact tables) and SCD T2 (dimension tables) as well as general logging.

![Data](/images/Dag.png "Airflow graph of tasks")

### Source (Lake)
This is a replayable data source for up to 14 days (storage concerns) and is an Amazon S3 bucket. Every day data from the previous day is moved to this central location in a folder named after the date that represents the day the the data is from. Today is May 1st 2007 so we are processing the data generated on April 30th 2007 in the morning before working hours. The entire tables for dimensional data is written because dimensions can change (eg: customer changes details, store changes location, product rating changes) and the processing system has to detect these changes by comparing them with existing data in dimensional tables to maintain SCD T2. **This is not the most efficient system and suggestions for improvement are written at the end**

![Data](/images/Lake1.png "Lake1")
![Data](/images/Lake2.png "Lake2")
![Data](/images/Lake3.png "Lake3")

### Sink (Warehouse)
The Data warehouse uses Kimball dimensional modelling. The fact table contains the base fact data (sale) and the dimensional tables contain dimensions of that fact. Dimensional tables has an insert_date column for SCD, which is also used in the primary key value. Fact table has a unique key payment_id (payment_date is included in PK because this table is partitioned on payment_date).

Dimension tables are upserted before insert into fact table so as to preserve foreign key RI constraints. There are indexes and partitions on this table schema to improve query performance more details are in `database/scripts/create_tables.sql`. During deployment of system a new user ABC is created for the Spark cluster to read and write data to Postgres instead of root user.

![Data](/images/Database%20schema.png "Database")

### Steps in the Pipeline: Cleaning, ETL, caching, deplication
Cleaning: 
1. Clean data in staging area
2. store it in transaction_data/batch_date/cleaned_data folder to preserve a copy of pre-validated data for investigation in pipeline failure autopsy

Dimensional transformation (improvments suggested in final section. Spark SQL does not recognise ON CONFLICT (col1) DO NOTHING):
1. Transform dimensional data
2. Read table from database
3. Remove duplicate rows from data (updated rows and new IDs are not removed, unchanged data is removed)
4. Add insert_date column with batch_date and load into database

Transaction transformation:
1. Transform transaction data
2. Add insert_date column with batch_date and load into database

![Data](/images/Dag.png "Airflow graph of tasks")

## The Pipeline Tech
### Spark and AWS
To minimize usage of storage space, the storage layer of the spark cluster is the AWS S3 staging area. I used the Hadoop-AWS integration module to connect my local spark cluster to AWS S3 (the required .jar files are in the Bitnami spark image). 

Pyspark scripts for each task in the pipeline are bind mounted to the Spark master node image. Airflow submits these scripts to Spark to run each task from validation to ETL.

### Replayability, Overwritability, and Idempotency
This pipelne has to be idempotent (Running a data pipeline numerous times with the same inputs will not create any duplicates or schema changes) becauese of real world errors that may occur and need to be fixed. Errors include writing wrong/duplicate data, batch job failures, partial writes, and so on. Idempotency allows us to go back and fix these errors in the data. Idempotency requires a **replayable source and overwritable sink**


## Potential improvements to this project
A quick word on how the technical functionality of this project can be improved with different setups.
- Data validation/cleaning should be done upon moving data to the S3 staging area for ETL from its source (limitations in this project)
- It is more efficient to use a hashing function to create a new column to deduplicate data instead of checking every field value in a row, while maintaining SCD Type 2 and Idempotency
- Research has to be done to find better ways to not transform dimensional data without transforming entities where no updates were made. A large amount of resources is spent every day transforming dimensional data, scanning for entities that are not new/updated, and excluding them from the database write. Perhaps a kind of monitoring system that monitors for entities which were updated, then the pipeline only transforms those entities and new additions to the database.
- Use a Columnar store database (Redshift/Snowflake/Bigquery) as the data warehouse for better query performance on big data
- Set up Spark on Kubernetes cluster or use cloud hosted Big data processing solution (eg: EMR, Databricks) for elastic scalability of workloads
- Ci/Cd pipeline. I can also consider using IAC to set up the whole system on the cloud


