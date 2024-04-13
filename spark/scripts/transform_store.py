import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DateType, FloatType
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
    
    df_cs = spark.read.options(header='true', inferSchema='true') \
    .csv(path=f"s3a://dvd-rental-data/dimension_data/customer.csv",
         schema=StructType([
            StructField('customer_id', IntegerType(), True),
            StructField('store_id', FloatType(), True),
            StructField('first_name', StringType(), True),
            StructField('last_name', StringType(), True),
            StructField('email', StringType(), True),
            StructField('address_id', FloatType(), True),
            StructField('activebool', StringType(), True),
            StructField('create_date', DateType(), True),
            StructField('last_update', TimestampType(), True),
            StructField('active', FloatType(), True)
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
    
    # Cache address, city, country USING ONLY NECESSARY COLUMNS TO SAVE CACHE SPACE
    df_add_cache = df_add.select('address','district','postal_code','address_id','city_id').cache()
    df_add_cache.count()
    df_co_cache = df_co.select('country','country_id').cache()
    df_co_cache.count()
    df_cy_cache = df_cy.select('city','city_id','country_id').cache()
    df_cy_cache.count()
    
    df_add_cache.createOrReplaceTempView('address')
    df_cy_cache.createOrReplaceTempView('city')
    df_co_cache.createOrReplaceTempView('country')
    df_st.createOrReplaceTempView('store')

    # Read dimension table from database
    dim_store = read_db(spark, 'dim_store')
    dim_customer = read_db(spark, 'dim_customer')
    print('dim_customer: ')
    dim_customer.show()
    dim_customer.printSchema()

    ##### FIRST STORE TABLE THEN CUSTOMER TABLE #######
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
    try:
        assert df_store.count() != 0
    except AssertionError as e:
        print("Error:", e)

    data_columns = ['store_id',
                    'address',
                    'district',
                    'postal_code',
                    'city',
                    'country']
    df_store.show()
    df_store = df_store.join(dim_store, data_columns, 'left_anti')
    df_store.show()
    df_store = df_store.withColumn('insert_date', lit(datetime.strptime(batch_date, '%Y-%m-%d').date()))
    df_store.show(); df_store.printSchema()
    #######################################################################################
    df_customer = spark.sql(
    """
    select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.active,
    a.address,
    a.district,
    ci.city,
    co.country

    from customer as c join address as a
        on c.address_id = a.address_id
    join city as ci
        on a.city_id = ci.city_id
    join country co 
        on ci.country_id = co.country_id
    """
    )
    try:
        assert df_store.count() != 0
    except AssertionError as e:
        print("Error:", e)

    data_columns = ['store_id',
                    'address',
                    'district',
                    'postal_code',
                    'city',
                    'country']
    df_customer.show()
    df_customer = df_customer.join(dim_customer, data_columns, 'left_anti')
    df_customer.show()
    df_customer = df_customer.withColumn('insert_date', lit(datetime.strptime(batch_date, '%Y-%m-%d').date()))
    df_customer.show(); df_customer.printSchema()
    aaa
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
    