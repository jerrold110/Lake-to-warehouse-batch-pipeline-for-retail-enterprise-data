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
In this system, a retail enterprise wants to load operational data into a data warehouse (dimensional model) from data stored in various different parts of their system on a daily basis for data analysis/business intelligence. The data pipeline has to be idempotent, scalable, and automated.

## The data
This diagram shows the different data sources, the transformations that need to take place, and the dimensional schema of the data warehouse. Assume that each table represents a different source stored in different locations. The red circle shows data used to form the Fact table, and the greed circles show data used to form dimensional tables.
![Data](/images/Data%20transformation.png "Pipeline")

### Dag, steps in each task, and daily pipeline timeline
### Source (Lake)
### Sink (Warehouse)

## The Pipeline
### Spark and AWS
### Replayability, Overwritability, and Idempotency

#### Potential improvements to this project
A quick word on how the technical functionality of this project can be improved with different setups.
- It is more efficient to use a hashing function to create a new column to deduplicate data instead of checking every field value in a row, while maintaining SCD Type 2 and Idempotency.
- Use a Columnar store database (Redshift/Snowflake/Bigquery) as the data warehouse for better query performance on big data
- Set up Spark on Kubernetes cluster or use cloud hosted Big data processing solution (eg: EMR, Databricks) for elastic scalability and fault-tolerance


