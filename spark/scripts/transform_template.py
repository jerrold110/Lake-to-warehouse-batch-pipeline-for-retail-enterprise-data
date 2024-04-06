import argparse
from pyspark.sql import SparkSession


def transform(batch_date:str):

    spark = SparkSession.builder\
    .appName(f"Transform...")\
    .master('spark://spark-master:7077')\
    .getOrCreate()
    
    # Read data

    # Read dimension table from database

    # Apply transformations with filter

    # Write data to db

    spark.stop()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_date") # string in the form yyyy-mm-dd
    args = parser.parse_args()

    transform(args.batch_date)
    