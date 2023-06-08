
import cx_Oracle
import psycopg2

# Oracle database connection details
oracle_username     = 'oracle_username'
oracle_password     = 'oracle_password'
oracle_host         = 'oracle_host.101.4.12'
oracle_port         = 'oracle_port'
oracle_service_name = 'oracle_service_name'

# PostgreSQL database connection details
postgres_username   = 'postgres_username'
postgres_password   = 'postgres_password'
postgres_host       = 'postgres_host.101.4.12'
postgres_port       = 'postgres_port'
postgres_database   = 'postgres_database'

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