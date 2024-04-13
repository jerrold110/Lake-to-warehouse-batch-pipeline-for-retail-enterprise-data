import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
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
    df_py = spark.read.options(header='true') \
    .csv(path=f"s3a://dvd-rental-data/transaction_data/{batch_date}/payment_cleaned",
        schema=StructType([
        StructField('payment_id', IntegerType(), True),
        StructField('customer_id', FloatType(), True),
        StructField('staff_id', FloatType(), True),
        StructField('rental_id', FloatType(), True),
        StructField('amount', FloatType(), True),
        StructField('payment_date', TimestampType(), True)
    ]))

    df_rt = spark.read.options(header='true') \
    .csv(path=f"s3a://dvd-rental-data/transaction_data/{batch_date}/rental_cleaned",
        schema=StructType([
        StructField('rental_id', IntegerType(), True),
        StructField('rental_date', TimestampType(), True),
        StructField('inventory_id', FloatType(), True),
        StructField('customer_id', FloatType(), True),
        StructField('return_date', TimestampType(), True),
        StructField('staff_id', FloatType(), True),
        StructField('last_update', TimestampType(), True)
    ]))

    df_iv = spark.read.options(header='true') \
    .csv(path=f"s3a://dvd-rental-data/dimension_data/inventory.csv",
         schema=StructType([
            StructField('inventory_id', IntegerType(), True),
            StructField('film_id', FloatType(), True),
            StructField('store_id', FloatType(), True),
            StructField('last_update', DateType(), True)
         ]))
    
    df_sf = spark.read.options(header='true') \
    .csv(path=f"s3a://dvd-rental-data/dimension_data/staff.csv",
         schema=StructType([
            StructField('staff_id', IntegerType(), True),
            StructField('first_name', StringType(), True),
            StructField('last_name', StringType(), True),
            StructField('address_id', FloatType(), True),
            StructField('email', StringType(), True),
            StructField('store_id', FloatType(), True)
         ]))
    
    df_iv.createOrReplaceTempView('inventory')
    df_py.createOrReplaceTempView('payment')
    df_rt.createOrReplaceTempView('rental')
    df_sf.createOrReplaceTempView('staff')

    # Read dimension table from database

    # Apply transformations with filter
        # Add insert_date to remaining rows in df
    df_sale = spark.sql(
    """
    select
    p.payment_id,
    p.customer_id,
    i.film_id,
    s.store_id,
    p.payment_date,
    p.amount as sale_amount,
    r.rental_date,
    r.return_date

    from payment as p join rental as r
        on p.rental_id = r.rental_id
    join inventory as i 
        on r.inventory_id = i.inventory_id
    join staff as s
        on p.staff_id = s.staff_id
    """
    )
    try:
        assert df_sale.count() != 0
    except AssertionError as e:
        print("Error:", e)

    df_sale = df_sale.withColumn('insert_date', lit(datetime.strptime(batch_date, '%Y-%m-%d').date()))
    df_sale.printSchema()
    df_sale.show()

    # Write data to db
    if df_sale.count() != 0:
        write_db(df_sale, 'fact_sale')
        print('Write complete')
    else:
        print('Nothing to write')

    spark.stop()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_date") # string in the form yyyy-mm-dd
    args = parser.parse_args()

    transform(args.batch_date)
    