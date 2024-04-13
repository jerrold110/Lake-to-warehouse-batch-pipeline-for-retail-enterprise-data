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
    df_cat = spark.read.options(header='true') \
    .csv(path=f"s3a://dvd-rental-data/dimension_data/category.csv",
        schema=StructType([
            StructField('category_id', IntegerType(), True),
            StructField('name', StringType(), True),
            StructField('last_update', TimestampType(), True)
        ]))
    
    df_fm = spark.read.options(header='true') \
    .csv(path=f"s3a://dvd-rental-data/dimension_data/film.csv",
         schema=StructType([
            StructField('film_id', IntegerType(), True),
            StructField('title', StringType(), True),
            StructField('description', StringType(), True),
            StructField('release_year', FloatType(), True),
            StructField('language_id', FloatType(), True),
            StructField('rental_duration', FloatType(), True),
            StructField('rental_rate', FloatType(), True),
            StructField('length', FloatType(), True),
            StructField('replacement_cost', FloatType(), True),
            StructField('rating', StringType(), True),
            StructField('last_update', DateType(), True)
         ]))
    
    df_fc = spark.read.options(header='true') \
    .csv(path=f"s3a://dvd-rental-data/dimension_data/film_category.csv",
         schema=StructType([
            StructField('film_id', IntegerType(), True),
            StructField('category_id', FloatType(), True),
            StructField('last_update', TimestampType(), True)
         ]))
    
    df_lg = spark.read.options(header='true') \
    .csv(path=f"s3a://dvd-rental-data/dimension_data/language.csv",
         schema=StructType([
            StructField('language_id', IntegerType(), True),
            StructField('name', StringType(), True),
            StructField('last_update', TimestampType(), True)
         ]))
    df_cat.createOrReplaceTempView('category')
    df_fm.createOrReplaceTempView('film')
    df_fc.createOrReplaceTempView('film_category')
    df_lg.createOrReplaceTempView('language')

    # Read dimension table from database
    dim_film = read_db(spark, 'dim_film')

    # Apply transformations with filter
        # Find rows in df with duplicate exact in dim. Remove entirely. Left with new/updated rows
        # Add insert_date to remaining rows in df
        
    df_film = spark.sql(
    """
    select
    f.film_id,
    f.title,
    f.description,
    f.release_year,
    f.rental_duration,
    f.rental_rate,
    f.length,
    f.replacement_cost,
    f.rating,
    l.name as language,
    c.name as category

    from film as f join language as l
        on f.language_id = l.language_id
    join film_category as fc
        on f.film_id = fc.film_id
    join category as c
        on fc.category_id = c.category_id
    """
    )
    try:
        assert df_film.count() != 0
    except AssertionError as e:
        print("Error:", e)

    data_columns = ['film_id',
                    'title',
                    'description',
                    'release_year',
                    'rental_duration',
                    'rental_rate',
                    'length',
                    'replacement_cost',
                    'rating',
                    'language',
                    'category']
    
    df_film = df_film.join(dim_film, data_columns, 'left_anti')
    df_film = df_film.withColumn('insert_date', lit(datetime.strptime(batch_date, '%Y-%m-%d').date()))
    
    # Write data to db
    if df_film.count() != 0:
        write_db(df_film, 'dim_film')
        print('Write complete')
    else:
        print('Nothing to write')

    spark.stop()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_date") # string in the form yyyy-mm-dd
    args = parser.parse_args()

    transform(args.batch_date)
    