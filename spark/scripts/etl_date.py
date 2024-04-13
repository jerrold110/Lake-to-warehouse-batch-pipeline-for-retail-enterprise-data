import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, to_date, unix_timestamp
from pyspark.sql.functions import year, month, dayofmonth, quarter, dayofweek
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DateType
from db_help import read_db, write_db


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
    df_py = spark.read.options(header='true', inferSchema='true') \
    .csv(path=f"s3a://dvd-rental-data/transaction_data/{batch_date}/payment_cleaned",
        schema=StructType([
        StructField('payment_id', IntegerType(), True),
        StructField('customer_id', IntegerType(), True),
        StructField('staff_id', IntegerType(), True),
        StructField('rental_id', IntegerType(), True),
        StructField('amount', FloatType(), True),
        StructField('payment_date', TimestampType(), True)
    ])).select('payment_date')

    df_rt = spark.read.options(header='true', inferSchema='true') \
    .csv(path=f"s3a://dvd-rental-data/transaction_data/{batch_date}/rental_cleaned",
        schema=StructType([
        StructField('rental_id', IntegerType(), True),
        StructField('rental_date', TimestampType(), True),
        StructField('inventory_id', IntegerType(), True),
        StructField('customer_id', IntegerType(), True),
        StructField('return_date', TimestampType(), True),
        StructField('staff_id', IntegerType(), True),
        StructField('last_update', TimestampType(), True)
    ])).select('rental_date', 'return_date')

    # Read dimension table from database
    dim_date = read_db(spark, 'dim_date')
    dim_date.printSchema()

    # Apply transformations with filter
    df_date = df_rt.select(col('rental_date').alias('date'))\
        .union(df_rt.select(col('return_date').alias('date')))\
        .union(df_py.select(col('payment_date').alias('date')))\
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))\
        .distinct()
    df_date = df_date.filter(df_date.date.isNotNull())
    df_date = df_date.withColumn('datekey', unix_timestamp('date').cast('int'))\
                .withColumn('year', year('date'))\
                .withColumn('month', month('date'))\
                .withColumn('day', dayofmonth('date'))\
                .withColumn('quarter', quarter('date'))\
                .withColumn('dayofweek', dayofweek('date'))
    try:
        assert df_date.count() != 0
    except AssertionError as e:
        print("Error:", e)
    df_date = df_date.join(dim_date,'datekey','left_anti')

    # Write data to db
    if df_date.count() != 0:
        write_db(df_date, 'dim_date')
        print('Write complete')
    else:
        print('Nothing to write')

    spark.stop()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_date") # string in the form yyyy-mm-dd
    args = parser.parse_args()

    transform(args.batch_date)
    