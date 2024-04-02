def load(job_timestamp):
    """
    Load job
    Writing to database is done with PostgreSQL JDBC Driver. Jar file is placed in /jars directory of $SPARK_HOME. Class is referenced with org.postgresql.Driver.
    After reading the data. Each dataframe is repartitioned into a number x, equal to the number of partitions in the database connector. 
    Read and write isolation level is serializable
    Apparently referring to the local path of the JDBC driver .jar file in SparkSession isn't necessary to initiate a DB connection
    """
    print('Load starting')
    import logging
    import py4j
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[*]") \
                        .appName('Data ETL') \
                        .getOrCreate()
    
    # Load transformed data
    prefix_file_path = f'./transformed_data/{job_timestamp}'

    dim_customer = spark.read.csv(f'{prefix_file_path}/dim_customer',
                                header=True,
                                schema='customer_id int, first_name string, last_name string, active int, address string, district string, city string, country string')

    dim_film = spark.read.csv(path=f'{prefix_file_path}/dim_film',
                            header=True,
                            schema='film_id int, title string, description string, release_year int, rental_duration int, rental_rate float, length int, replacement_cost float, rating string, language string, category string')

    dim_store = spark.read.csv(path=f'{prefix_file_path}/dim_store',
                            header=True,
                            schema='store_id int, address string, district string, postal_code int, city string, country string')

    dim_date = spark.read.csv(path=f'{prefix_file_path}/dim_date',
                            header=True,
                            schema='date timestamp, datekey int, year int, month int, day int, quarter int, dayofweek int')

    fact_sale = spark.read.csv(path=f'{prefix_file_path}/fact_sale',
                            header=True,
                            schema='payment_id int, customer_id int, film_id int, store_id int, payment_date timestamp, sale_amount float, rental_date timestamp, return_date timestamp')

    
    no_partitions = 4
    dim_customer.repartition(no_partitions)
    dim_film.repartition(no_partitions)
    dim_store.repartition(no_partitions)
    dim_date.repartition(no_partitions)
    fact_sale.repartition(no_partitions)

    # Posgres read and write functions for each table
    # Transaction isolation level is serializable
    ###############################################################################################################

    def read_db(table):
        """
        Returns a table as a dataframe. db: alabama db_vendor: posgresql
        """
        postgres_url = 'jdbc:postgresql://localhost:5432/alabama'
        properties = {
            "user": "my_user",
            "password": "my_user_1",
            "driver": "org.postgresql.Driver",
            "isolationLevel": "SERIALIZABLE"
        }
        
        return spark.read.jdbc(url=postgres_url, table=table, properties=properties)

    def write_df(df, table, partitions):
        """
        Write appends a dataframe as a table. db: alabama db_vendor: posgresql
        """
        postgres_url = 'jdbc:postgresql://localhost:5432/alabama'
        properties = {
            "user": "my_user",
            "password": "my_user_1",
            "driver": "org.postgresql.Driver",
            "numPartitions": str(partitions), # equal to or lesser than the no. partitions of the DF
            "isolationLevel": "SERIALIZABLE"
        }
        try:
            logging.error(f'Writing dataframe to {table} table')
            df.write.jdbc(url=postgres_url, table=table, mode="append", properties=properties) # change mode to ignore to test
        except py4j.protocol.Py4JJavaError as e:
            logging.error(e)
        except Exception as e:
            logging.error(e)
        
    def filter_load_dim_df(df, table, partitions, idx_col):
        """
        Function for filtering out existing rows in the dimension tables before updating the data.
        """
        # Read table
        print(f'Reading {table}')
        existing_table = read_db(table)
        
        # Filter out existing ids
        print(f'Filtering rows from {table}')
        non_existing_rows = df.join(existing_table, [idx_col], "leftanti")
        
        # Load rows with ids not present in the database
        print(f'Loading {table}')
        write_df(non_existing_rows, table, partitions)
        print(f'Load for {table} done')
        
    def load_fact_df(df, table, partitions):
        print(f'Loading {table}')
        write_df(df, table, partitions)
        print(f'Load for {table} done')
    ###############################################################################################################
    # no_partitions from before
    filter_load_dim_df(dim_customer, 'dim_customer', no_partitions, 'customer_id')
    filter_load_dim_df(dim_film, 'dim_film', no_partitions, 'film_id')
    filter_load_dim_df(dim_store, 'dim_store', no_partitions, 'store_id')
    filter_load_dim_df(dim_date, 'dim_date', no_partitions, 'datekey')
    load_fact_df(fact_sale, 'fact_sale', no_partitions)

    spark.stop()

if __name__ == '__main__':
    from datetime import datetime
    from date_filepath import date_filepath
    job_timestamp = date_filepath(datetime(2011, 1, 1, 0, 0, 0))
    load(job_timestamp)
    