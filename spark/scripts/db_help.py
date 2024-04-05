import py4j
from pyspark.sql import SparkSession    

def read_db(spark, table:str):

    df = spark.read.format('jdbc')\
        .option('url', 'jdbc:postgresql://localhost:5432/postgres')\
        .option('dbtable', table)\
        .option('driver', 'org.postgresql.Driver')\
        .option('user', 'abc')\
        .option('password', 'abc')\
        .load()
    
    return df

# def write_df(df, table, partitions):
#     """
#     Write appends a dataframe as a table. db: alabama db_vendor: posgresql
#     """
#     postgres_url = 'jdbc:postgresql://localhost:5432/alabama'
#     properties = {
#         "user": "my_user",
#         "password": "my_user_1",
#         "driver": "org.postgresql.Driver",
#         "numPartitions": str(partitions), # equal to or lesser than the no. partitions of the DF
#         "isolationLevel": "SERIALIZABLE"
#     }
#     try:
#         df.write.jdbc(url=postgres_url, table=table, mode="append", properties=properties) # change mode to ignore to test
#     except py4j.protocol.Py4JJavaError as e:
#         print(e)
#     except Exception as e:
#         print(e)
    
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