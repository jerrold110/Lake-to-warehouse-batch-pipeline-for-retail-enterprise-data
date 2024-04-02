def transform(job_timestamp):
    """
    Transform job
    
    Reasons for writing the transformed data to persistent storage instead of passing between functions:
    - Data persists after spark sessions ends, whether due to error or crash
    - Decouples transform and load operations
    - Persisting the data allows for parallel and distributed processing
    """
    print('Transform starting')
    from pyspark.sql import SparkSession
    import logging
    spark = SparkSession.builder.master("local[*]") \
                        .appName('Data ETL') \
                        .getOrCreate()
    prefix_file_path = f'./extracted_data/{job_timestamp}'
    print(prefix_file_path)
    df_add = spark.read.csv(f'{prefix_file_path}/address.dat',
                        sep='\t',
                        schema='address_id int, address string, address2 string, district string, city_id int, postal_code int, phone string, last_update timestamp')

    df_cat = spark.read.csv(f'{prefix_file_path}/category.dat',
                        sep='\t',
                        schema='category_id int, name string, last_update timestamp')

    df_co = spark.read.csv(f'{prefix_file_path}/country.dat',
                        sep='\t',
                        schema='country_id int, country string, last_update timestamp')

    df_cy = spark.read.csv(f'{prefix_file_path}/city.dat',
                        sep='\t',
                        schema='city_id int, city string, country_id int, last_update timestamp')

    df_cs = spark.read.csv(f'{prefix_file_path}/customer.dat',
                        sep='\t',
                        schema="""customer_id int, store_id int, first_name string, last_name string, 
                        email string, address_id int, activebool string, create_date date, last_update timestamp, active byte""")
    
    df_fm = spark.read.csv(f'{prefix_file_path}/film.dat',
                        sep='\t',
                        schema="""film_id int, title string, description string, release_year int, language_id int, rental_duration int, rental_rate float, 
                                length int, replacement_cost float, rating string, last_update date""")

    df_fc = spark.read.csv(f'{prefix_file_path}/film_category.dat',
                        sep='\t',
                        schema='film_id int, category_id int, last_update timestamp')

    df_iv = spark.read.csv(f'{prefix_file_path}/inventory.dat',
                        sep='\t',
                        schema='inventory_id int, film_id int, store_id int, last_update date')

    df_lg = spark.read.csv(f'{prefix_file_path}/language.dat',
                        sep='\t',
                        schema='language_id int, name string, last_update timestamp')

    df_py = spark.read.csv(f'{prefix_file_path}/payment.dat',
                        sep='\t',
                        schema='payment_id int, customer_id int, staff_id int, rental_id int, amount float, payment_date timestamp')

    df_rt = spark.read.csv(f'{prefix_file_path}/rental.dat',
                        sep='\t',
                        schema='rental_id int, rental_date timestamp, inventory_id int, customer_id int, return_date timestamp, staff_id int, last_update timestamp')

    df_sf = spark.read.csv(f'{prefix_file_path}/staff.dat',
                        sep='\t',
                        schema='staff_id int, first_name string, last_name string, address_id int, email string, store_id int')

    df_st = spark.read.csv(f'{prefix_file_path}/store.dat',
                        sep='\t',
                        schema='store_id int, manager_staff_id int, address_id int')
    print('File reads from extracted_data completed')

    # Cleaning dataframes of null rows
    # Caching dataframes that are used more than once
    # Repartitioning dataframes with many rows

    df_add = df_add.na.drop("all").cache()
    df_cat = df_cat.na.drop("all")
    df_cy = df_cy.na.drop("all").cache()
    df_co = df_co.na.drop('all')
    df_cs = df_cs.na.drop("all")
    df_fm = df_fm.na.drop("all").repartition(8).drop()
    df_fc = df_fc.na.drop("all")
    df_iv = df_iv.na.drop("all").drop().cache()
    df_lg = df_lg.na.drop("all").cache()
    df_py = df_py.na.drop("all").repartition(8).cache()
    df_rt = df_rt.na.drop("all").repartition(8)
    df_sf = df_sf.na.drop("all")
    df_st = df_st.na.drop("all")

    df_add.createOrReplaceTempView('address')
    df_cat.createOrReplaceTempView('category')
    df_cy.createOrReplaceTempView('city')
    df_co.createOrReplaceTempView('country')
    df_cs.createOrReplaceTempView('customer')
    df_fm.createOrReplaceTempView('film')
    df_fc.createOrReplaceTempView('film_category')
    df_iv.createOrReplaceTempView('inventory')
    df_lg.createOrReplaceTempView('language')
    df_py.createOrReplaceTempView('payment')
    df_rt.createOrReplaceTempView('rental')
    df_sf.createOrReplaceTempView('staff')
    df_st.createOrReplaceTempView('store')

    dim_customer = spark.sql(
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

    dim_film = spark.sql(
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

    dim_store = spark.sql(
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

    from pyspark.sql.functions import year, month, dayofmonth, quarter, dayofweek
    from pyspark.sql.functions import col, date_format, to_date, unix_timestamp

    df_py_date = df_py.withColumn("date", to_date(col("payment_date"), "yyyy-MM-dd"))

    dim_date = df_py_date.select('date').distinct()
    dim_date = dim_date.withColumn('datekey', unix_timestamp('date').cast('long'))\
                .withColumn('year', year('date'))\
                .withColumn('month', month('date'))\
                .withColumn('day', dayofmonth('date'))\
                .withColumn('quarter', quarter('date'))\
                .withColumn('dayofweek', dayofweek('date'))

    fact_sale = spark.sql(
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
    print('Data transformations complete')

    #Checking that none of the dataframes are empty
    dfs = ((dim_customer,'dim_customer'),(dim_film, 'dim_film'),(dim_store, 'dim_store'),(dim_date, 'dim_date'),(fact_sale, 'fact_sale'))
    for df_ in dfs:
        logging.error((f'Successfully created {df_[1]} dataframe'))
        length = df_[0].count()
        logging.error((f'{df_[1]} contains {df_[1]} rows'))
        if df_[0].count() <= 1:
            logging.error(f'Rows of {df_[1]} are <= 1')

    dim_customer.repartition(10).write.csv(f'./transformed_data/{job_timestamp}/dim_customer', mode='overwrite', header=True)
    dim_film.repartition(10).write.csv(f'./transformed_data/{job_timestamp}/dim_film', mode='overwrite', header=True)
    dim_store.repartition(10).write.csv(f'./transformed_data/{job_timestamp}/dim_store', mode='overwrite', header=True)
    dim_date.repartition(10).write.csv(f'./transformed_data/{job_timestamp}/dim_date', mode='overwrite', header=True)
    fact_sale.repartition(10).write.csv(f'./transformed_data/{job_timestamp}/fact_sale', mode='overwrite', header=True)

    #print((fact_sale.count(), len(fact_sale.columns)))
    spark.stop()
    print('Data file writes to transformed_data complete')

if __name__ == '__main__':
    from datetime import datetime
    from date_filepath import date_filepath
    job_timestamp = date_filepath(datetime(2011, 1, 1, 0, 0, 0))
    transform(job_timestamp)
