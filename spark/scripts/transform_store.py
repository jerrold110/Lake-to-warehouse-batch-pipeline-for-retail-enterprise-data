import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DateType
from db_help import read_db


def transform(batch_date:str):

    spark = SparkSession.builder\
    .appName(f"Transform...")\
    .config("spark.jars", '/opt/bitnami/spark/jars/postgresql-42.7.3.jar')\
    .master('spark://spark-master:7077')\
    .getOrCreate()
    
    # Read data
    # df_add = spark.read.options(header='true', inferSchema='true') \
    # .csv(path=f"s3a://dvd-rental-data/dimension_data/address.csv",
    #     schema=StructType([
    #         StructField('address_id', IntegerType(), True),
    #         StructField('address', StringType(), True),
    #         StructField('address2', StringType(), True),
    #         StructField('district', StringType(), True),
    #         StructField('city_id', IntegerType(), True),
    #         StructField('postal_code', IntegerType(), True),
    #         StructField('phone', StringType(), True),
    #         StructField('last_update', TimestampType(), True)
    #     ]))
    
    # df_co = spark.read.options(header='true', inferSchema='true') \
    # .csv(path=f"s3a://dvd-rental-data/dimension_data/country.csv",
    #      schema=StructType([
    #         StructField('country_id', IntegerType(), True),
    #         StructField('country', StringType(), True),
    #         StructField('last_update', TimestampType(), True)
    #      ]))

    # df_cy = spark.read.options(header='true', inferSchema='true') \
    #     .csv(path=f"s3a://dvd-rental-data/dimension_data/city.csv",
    #         schema=StructType([
    #             StructField('city_id', IntegerType(), True),
    #             StructField('city', StringType(), True),
    #             StructField('country_id', IntegerType(), True),
    #             StructField('last_update', TimestampType(), True)
    #         ]))

    # df_st = spark.read.options(header='true', inferSchema='true') \
    # .csv(path=f"s3a://dvd-rental-data/dimension_data/store.csv",
    #      schema=StructType([
    #         StructField('store_id', IntegerType(), True),
    #         StructField('manager_staff_id', IntegerType(), True),
    #         StructField('address_id', IntegerType(), True)
    #      ]))
    
    # Read dimension table from database
    dim_store = read_db(spark, 'dim_store')
    dim_store.show()
    dim_store.printSchema()

    # Apply transformations
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')

    # Write data to db

    spark.stop()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_date") # string in the form yyyy-mm-dd
    args = parser.parse_args()

    transform(args.batch_date)
    