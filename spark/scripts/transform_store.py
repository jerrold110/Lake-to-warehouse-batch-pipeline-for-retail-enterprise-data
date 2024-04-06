import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
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
    df_add = spark.read.options(header='true', inferSchema='true') \
    .csv(path=f"s3a://dvd-rental-data/dimension_data/address.csv",
        schema=StructType([
            StructField('address_id', IntegerType(), True),
            StructField('address', StringType(), True),
            StructField('address2', StringType(), True),
            StructField('district', StringType(), True),
            StructField('city_id', FloatType(), True),
            StructField('postal_code', FloatType(), True),
            StructField('phone', FloatType(), True),
            StructField('last_update', TimestampType(), True)
        ]))
    df_co = spark.read.options(header='true', inferSchema='true') \
    .csv(path=f"s3a://dvd-rental-data/dimension_data/country.csv",
         schema=StructType([
            StructField('country_id', IntegerType(), True),
            StructField('country', StringType(), True),
            StructField('last_update', TimestampType(), True)
         ]))

    df_cy = spark.read.options(header='true', inferSchema='true') \
        .csv(path=f"s3a://dvd-rental-data/dimension_data/city.csv",
            schema=StructType([
                StructField('city_id', IntegerType(), True),
                StructField('city', StringType(), True),
                StructField('country_id', FloatType(), True),
                StructField('last_update', TimestampType(), True)
            ]))

    df_st = spark.read.options(header='true', inferSchema='true') \
    .csv(path=f"s3a://dvd-rental-data/dimension_data/store.csv",
         schema=StructType([
            StructField('store_id', IntegerType(), True),
            StructField('manager_staff_id', FloatType(), True),
            StructField('address_id', FloatType(), True),
            StructField('last_update', TimestampType(), True)
         ]))
    #df_add.show()
    #df_co.show()
    #df_cy.show()
    #df_st.show()
    
    df_add.createOrReplaceTempView('address')
    df_cy.createOrReplaceTempView('city')
    df_co.createOrReplaceTempView('country')
    df_st.createOrReplaceTempView('store')

    # Read dimension table from database
    dim_store = read_db(spark, 'dim_store')
    dim_store.show()
    dim_store.printSchema()

    # Apply transformations with filter
        # Find rows in df with duplicate exact in dim. Remove entirely. Left with new/updated rows
        # Add insert_date to remaining rows in df
    df_store = spark.sql(
    """
    select
    s.store_id,
    a.address,
    a.district,
    a.postal_code,
    c.city,
    co.country

    from store as s join address as a
        on s.address_id = a.address_id
    join city as c
        on a.city_id = c.city_id
    join country as co
        on c.country_id = co.country_id
    """
    )
    df_store.show()
    assert df_store.count() != 0

    data_columns = ['store_id'
                    'address',
                    'district',
                    'postal_code',
                    'city',
                    'country']
    df_store.show(); df_store.printSchema()
    df_store = df_store.join(dim_store, data_columns, 'left_anti')
    df_store.show(); df_store.printSchema()
    df_store = df_store.with_column('insert_date', lit(datetime(batch_date).cast(DateType())))
    df_store.show(); df_store.printSchema()

    # Write data to db
    write_db(spark, df_store, 'dim_store')

    spark.stop()

if __name__ == '__main__':


    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_date") # string in the form yyyy-mm-dd
    args = parser.parse_args()

    transform(args.batch_date)
    