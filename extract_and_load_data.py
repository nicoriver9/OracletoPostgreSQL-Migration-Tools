import cx_Oracle
import psycopg2
from psycopg2 import Error
from dotenv import load_dotenv
import os


load_dotenv()

# Oracle database connection details
oracle_username     = os.getenv("ORACLE_USERNAME")
oracle_password     = os.getenv("ORACLE_PASSWORD")
oracle_host         = os.getenv("ORACLE_HOST")
oracle_port         = os.getenv("ORACLE_PORT")
oracle_service_name = os.getenv("ORACLE_SERVICE_NAME")

# PostgreSQL database connection details
postgres_username   = os.getenv("POSTGRES_USERNAME")
postgres_password   = os.getenv("POSTGRES_PASSWORD")
postgres_host       = os.getenv("POSTGRES_HOST")
postgres_port       = os.getenv("POSTGRES_PORT")
postgres_database   = os.getenv("POSTGRES_DATABASE")

# Tables mapping from Oracle to PostgreSQL
table_mapping = {
    # "oracle_table_1"           : 'postgres_table_1',
    # "oracle_table_2"           : 'postgres_table_2',
    # Add more table mappings as needed
}

try:
    # Establish Oracle database connection
    oracle_dsn = cx_Oracle.makedsn(oracle_host, oracle_port, service_name=oracle_service_name)
    oracle_conn = cx_Oracle.connect(oracle_username, oracle_password, dsn=oracle_dsn)

    # Establish PostgreSQL database connection
    postgres_conn = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        database=postgres_database,
        user=postgres_username,
        password=postgres_password
    )
    print("one moment please...")
    # Iterate over table mappings and perform data transfer
    for oracle_table, postgres_table in table_mapping.items():
        try:
            # Select data from Oracle table
            oracle_cursor = oracle_conn.cursor()
            oracle_cursor.execute(f"SELECT * FROM {oracle_table}")
            oracle_data = oracle_cursor.fetchall()
            oracle_cursor.close()

            # Insert data into PostgreSQL table
            postgres_cursor = postgres_conn.cursor()
            placeholders = ','.join(['%s'] * len(oracle_data[0]))
            postgres_cursor.executemany(f"INSERT INTO {postgres_table} VALUES ({placeholders})", oracle_data)
            postgres_conn.commit()
            postgres_cursor.close()

            print(f"Data transferred from {oracle_table} to {postgres_table} successfully.")

        except (cx_Oracle.Error, psycopg2.Error) as error:
            print(f"Error transferring data from {oracle_table} to {postgres_table}: {error}")

except (cx_Oracle.Error, psycopg2.Error) as error:
    print("Error connecting to databases:", error)

finally:
    # Close database connections
    if 'oracle_conn' in locals() and oracle_conn is not None:
        oracle_conn.close()

    if 'postgres_conn' in locals() and postgres_conn is not None:
        postgres_conn.close()
