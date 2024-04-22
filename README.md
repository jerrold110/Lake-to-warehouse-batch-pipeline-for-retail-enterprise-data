# Lake to warehouse batch pipeline for retail enterprise data

#### Run this project
```
docker compose up -d
./setup.sh

# Spark UI: localhost:8080
# Airflow Webserver: localhost:8081
# PostgreSQL: localhost:5432
```
## Overview
In this system, a DVD rental retail enterprise wants to load operational data into a data warehouse (dimensional model) from data stored in various different parts of their system on a daily basis for data analysis/business intelligence. The data pipeline has to be <u>idempotent, scalable, low cloud-cost, and automated</u> with Type 2 Slowly Changing Dimension so as to not lose data in the past as dimensions are updated.

Components:
- Data lake (assume the data from various sources are consolidated here before pipeline begins)
- Data processor 
- Data Warehouse (stores all historical data)
- Data Mart (Analysts use this. **It is a copy of the data warehouse, or subset of it, that is written to only when pipeline End state is successful**)

## The data
This diagram shows the different data sources, the transformations that need to take place, and the dimensional schema of the data warehouse. Assume that each table represents a different source stored in different locations. The red circle shows data used to form the Fact table, and the green circles show data used to form dimensional tables.
![Data](/images/Data%20transformation.png "Pipeline")

There sources are categorised into two types of data that are treated differently because of the scale at which new records arrive. Dimensional data and Transactional data.
Dimensional data (customer, product, outlet) has only a small number of additions compared to transactional data (sales, payments, inventory)

### Dag, steps in each task, and daily pipeline timeline
The pipeline runs on an automated schedule every day after the storage of the previous day's data is completed so as to make yesterday's data immediately available for analysis the next day. This is schedule of jobs at this company:
- 00:00 Logical end of the previous day's data. All writes after this are recorded with a different date
- 00:10 Data from various sources in the system are copied into the data lake to prepare for pipeline start
- 03:00 All OLTP writes are complete (assuming that even large numbers of data collected are complete by 3am because system is scalable)
- 03:10 Pipeline started and ETL process is underway
- 06:00 Pipeline is completed and status of the Airflow tasks are logged. Pipeline is either success or fail. Data Mart is updated only if entire pipeline is a success.

#### DAG on Airflow
This diagram shows the DAG of the tasks in this pipeline and how airflow orchestrates them. There are data validation tasks for the day's data to ensure that data is clean and available for transformation. This should be extended for dimensional data as well, not just transactional data. ETL tasks (cashing, deduplication, transformation, load) which operate on dimensional table data BEFORE fact table data so as to meet referential integrity constraints in the DWH. 
![Data](/images/Dag.png "Airflow graph of tasks")

### Source (Lake)
![Data](/images/Lake1.png "Lake1")
![Data](/images/Lake2.png "Lake2")
![Data](/images/Lake3.png "Lake3")

### Sink (Warehouse)
![Data](/images/Database%20schema.png "Database")

## The Pipeline
### Spark and AWS
### Replayability, Overwritability, and Idempotency

#### Potential improvements to this project
A quick word on how the technical functionality of this project can be improved with different setups.
- It is more efficient to use a hashing function to create a new column to deduplicate data instead of checking every field value in a row, while maintaining SCD Type 2 and Idempotency.
- Use a Columnar store database (Redshift/Snowflake/Bigquery) as the data warehouse for better query performance on big data
- Set up Spark on Kubernetes cluster or use cloud hosted Big data processing solution (eg: EMR, Databricks) for elastic scalability and fault-tolerance
- Ci/Cd pipeline


