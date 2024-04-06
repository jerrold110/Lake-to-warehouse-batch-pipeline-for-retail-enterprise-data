import psycopg2
from psycopg2 import OperationalError

def check_postgresql_connection(host, port, dbname, user, password):
    try:
        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )

        # Check if the connection is successful
        if conn:
            print("Connection to PostgreSQL successful!")
            conn.close()  # Close the connection
    except OperationalError as e:
        print(f"Error: {e}")

# Replace these variables with your PostgreSQL connection details
host = 'localhost'  # Hostname or IP address
port = '5432'       # Port number
dbname = 'dvd_database'  # Name of your database
user = 'abc'         # Your username
password = 'abc'    # Your password

# Call the function to check the PostgreSQL connection
check_postgresql_connection(host, port, dbname, user, password)
