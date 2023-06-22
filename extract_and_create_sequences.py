
import cx_Oracle
import psycopg2
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

# Tables to copy sequences for
table_list = [
        "ORACLE_TABLE"
        
        ]  # Add the desired table names here

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


# Copy sequences from Oracle to PostgreSQL
for table_name in table_list:
    try:
        # Get the sequence details from Oracle data dictionary views
        oracle_cursor = oracle_conn.cursor()
        oracle_cursor.execute(f"SELECT sequence_name, last_number FROM user_sequences WHERE sequence_name = 'SEQ_{table_name}'")
        sequence_data = oracle_cursor.fetchone()
        oracle_cursor.close()

        if sequence_data:
            sequence_name, last_number = sequence_data

            # Create the equivalent sequence in PostgreSQL
            postgres_cursor = postgres_conn.cursor()
            postgres_cursor.execute(f"CREATE SEQUENCE seq_{table_name} START {last_number}")
            postgres_conn.commit()
            postgres_cursor.close()

            print(f"Sequence {table_name}_seq copied successfully.")
        else:
            print(f"Sequence {table_name}_SEQ does not exist in Oracle.")

    except (cx_Oracle.Error, psycopg2.Error) as error:
        print(f"Error copying sequence {table_name}_seq: {error}")

# Close database connections
oracle_conn.close()
postgres_conn.close()