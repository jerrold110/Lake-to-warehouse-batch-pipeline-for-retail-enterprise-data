"""
This file contains read and write helper functions for the database

"""
import py4j
import pyspark
from pyspark.sql import SparkSession

def read_db(spark, table:str):

    df = spark.read.format('jdbc')\
            .option('url', 'jdbc:postgresql://postgres-db:5432/dvd_database')\
            .option('dbtable', table)\
            .option('driver', 'org.postgresql.Driver')\
            .option('user', 'abc')\
            .option('password', 'abc')\
            .load()

    return df

def write_db(df:pyspark.sql.dataframe.DataFrame, db_table:str):

    try:
        df.write.format('jdbc').mode('append')\
        .option('url', 'jdbc:postgresql://postgres-db:5432/dvd_database')\
        .option('dbtable', db_table)\
        .option('driver', 'org.postgresql.Driver')\
        .option('user', 'abc')\
        .option('password', 'abc')\
        .option('isolationlevel', 'SERIALIZABLE')\
        .save()
    except py4j.protocol.Py4JJavaError as e:
        print(e)
    except Exception as e:
        print('Error:', e)
    
# def filter_load_dim_df(df, table, partitions, idx_col):
#     """
#     Function for filtering out existing rows in the dimension tables before updating the data.
#     """
#     # Read table
#     print(f'Reading {table}')
#     existing_table = read_db(table)
    
#     # Filter out existing ids
#     print(f'Filtering rows from {table}')
#     non_existing_rows = df.join(existing_table, [idx_col], "leftanti")
    
#     # Load rows with ids not present in the database
#     print(f'Loading {table}')
#     write_df(non_existing_rows, table, partitions)
#     print(f'Load for {table} done')
    
# def load_fact_df(df, table, partitions):
#     print(f'Loading {table}')
#     write_df(df, table, partitions)
#     print(f'Load for {table} done')