import argparse
from pyspark.sql import SparkSession


def transform(batch_date:str):

    spark = SparkSession.builder\
    .appName(f"Transform...")\
    .config("spark.jars", '/opt/bitnami/spark/jars/postgresql-42.7.3.jar')\
    .master('spark://spark-master:7077')\
    .getOrCreate()
    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getLogger('org').setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger('akka').setLevel(logger.Level.ERROR)
    
    # Read data

    # Read dimension table from database

    # Apply transformations with filter
        # Find rows in df with duplicate exact in dim. Remove entirely. Left with new/updated rows
        # Add insert_date to remaining rows in df

    # Write data to db
    if df_store.count() != 0:
        write_db(df_store, 'dim_store')
        print('Write complete')
    else:
        print('Nothing to write')

    spark.stop()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_date") # string in the form yyyy-mm-dd
    args = parser.parse_args()

    transform(args.batch_date)
    