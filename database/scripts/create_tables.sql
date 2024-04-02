/*
drop table fact_sale;
drop table dim_store;
drop table dim_film;
drop table dim_date;
drop table dim_customer;
*/

CREATE TABLE IF NOT EXISTS dim_film (
    film_id INT,
    title VARCHAR(255),
    description VARCHAR(255),
    release_year INT,
    rental_duration INT,
    rental_rate DECIMAL(10,2),
    length INT,
    replacement_cost FLOAT,
    rating CHAR(20),
    language CHAR(50),
    category CHAR(255),
    PRIMARY KEY (film_id)
    --CONSTRAINT unique_film_id UNIQUE (film_id)
);
CREATE INDEX idx_df_film_id ON dim_film(film_id);
CREATE INDEX idx_df_rating ON dim_film(rating);

CREATE TABLE IF NOT EXISTS dim_customer(
    customer_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    active SMALLINT,
    address VARCHAR(255),
    district VARCHAR(50),
    city VARCHAR(50),
    country VARCHAR(50),
    PRIMARY KEY (customer_id)
);
CREATE INDEX idx_dc_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_dc_city ON dim_customer(city);
CREATE INDEX idx_dc_country ON dim_customer(country);

CREATE TABLE IF NOT EXISTS dim_date(
    date TIMESTAMP,
    datekey INT,
    year INT,
    month INT,
    day INT,
    quarter INT,
    dayofweek INT,
    PRIMARY KEY (datekey)
);
CREATE INDEX idx_dd_datekey ON dim_date(datekey);
CREATE INDEX idx_dd_year_month ON dim_date(year, month);

CREATE TABLE IF NOT EXISTS dim_store(
    store_id INT,
    address CHAR(255),
    district CHAR(255),
    postal_code INT,
    city CHAR(50),
    country CHAR(50),
    PRIMARY KEY (store_id)
);
CREATE INDEX idx_ds_store_id ON dim_store(store_id);
CREATE INDEX idx_ds_city ON dim_store(city);
CREATE INDEX idx_ds_country ON dim_store(country);

-- Create Partitions on payment_date
CREATE TABLE IF NOT EXISTS fact_sale(
    payment_id INT,
    customer_id INT,
    film_id INT,
    store_id INT,
    payment_date TIMESTAMP,
    sale_amount DECIMAL(10,2),
    rental_date TIMESTAMP,
    return_date TIMESTAMP,
    -- primary and foreign keys of the fact table
    PRIMARY KEY (payment_id, payment_date),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (film_id) REFERENCES dim_film(film_id),
    FOREIGN KEY (store_id) REFERENCES dim_store(store_id)
)
PARTITION BY RANGE (payment_date);
-- Partition tables with declarative partitioning
CREATE TABLE fact_sale_2007h1 PARTITION OF fact_sale
    FOR VALUES FROM ('2007-01-01') TO ('2007-06-01');
CREATE TABLE fact_sale_2007h2 PARTITION OF fact_sale
    FOR VALUES FROM ('2007-06-02') TO ('2007-12-30');
CREATE INDEX idx_fs_payment_date on fact_sale(payment_date);
-- Indexes
CREATE INDEX idx_fs_customer_id ON fact_sale(customer_id);
CREATE INDEX idx_fs_film_id ON fact_sale(film_id);
CREATE INDEX idx_fs_store_id ON fact_sale(store_id);
