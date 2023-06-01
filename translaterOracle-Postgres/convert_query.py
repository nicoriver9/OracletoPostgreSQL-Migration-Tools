import cx_Oracle
import re


# Oracle database connection details
oracle_username = 'your_oracle_username'
oracle_password = 'your_oracle_password'
oracle_host = 'your_oracle_host'
oracle_port = 'your_oracle_port'
oracle_service_name = 'your_oracle_service_name'

# PostgreSQL database connection details
postgres_username = 'your_postgres_username'
postgres_password = 'your_postgres_password'
postgres_host = 'your_postgres_host'
postgres_port = 'your_postgres_port'
postgres_database = 'your_postgres_database'


postgres_schema = 'schema'
postgres_table = 'table_name'

def extract_table_creation_query(oracle_connection, table_name):
    cursor = oracle_connection.cursor()

    # Obtener la consulta de creación de la tabla de Oracle
    cursor.execute(f"SELECT DBMS_METADATA.GET_DDL('TABLE', '{table_name.upper()}') FROM DUAL")
    ddl_lob = cursor.fetchone()[0]
    ddl = ddl_lob.read()
    # Cerrar el cursor y la conexión a Oracle
    cursor.close()
    oracle_connection.close()

    return ddl


def convert_query(oracle_query, postgres_schema, postgres_table):
    if not isinstance(oracle_query, str):
        raise ValueError("La consulta de Oracle debe ser una cadena de texto.")

    # Buscar la parte de la consulta que contiene las columnas y tipos de datos
    match = re.search(r'\(\s*(.*?)\s*\)\s*(?:PRIMARY\s+KEY\s+\w+\s*\(.*?\))?\s*(?:USING\s+INDEX\s+.*?\s*)?(?:TABLESPACE\s+\w+\s*)?;?$', oracle_query, re.DOTALL | re.IGNORECASE)
    if not match:
        raise ValueError("No se encontraron columnas en la consulta de Oracle.")

    # Obtener la parte de la consulta con las columnas y tipos de datos
    column_section = match.group(1)

    # Extraer los nombres de las columnas y tipos de datos
    columns = re.findall(r'"?(\w+)"?\s+([\w()]+(?:\s*\([^)]*\))?)', column_section, re.IGNORECASE)

    # Mapear los tipos de datos de Oracle a PostgreSQL
    type_mapping = {
        'NUMBER': 'NUMERIC',
        'VARCHAR2': 'VARCHAR',
        'DATE': 'DATE'
        # Agrega aquí otros mapeos necesarios para tipos de datos adicionales
    }

    # Construir la consulta de creación de tabla de PostgreSQL
    pg_query = f'CREATE TABLE {postgres_schema}.{postgres_table} (\n'
    pg_columns = []

    for i, column in enumerate(columns):
        column_name = column[0]
        column_type = column[1]

        # Reemplazar el tipo de datos de Oracle por el equivalente en PostgreSQL
        for oracle_type, pg_type in type_mapping.items():
            column_type = column_type.replace(oracle_type, pg_type)

        # Extraer los números dentro de los paréntesis en las columnas de tipo NUMERIC
        if 'NUMERIC' in column_type:
            num_match = re.search(r'NUMERIC\s*\((.*?)\)', column_type)
            if num_match:
                num_value = num_match.group(1).replace(' ', '')
                column_type = column_type.replace(num_match.group(1), num_value)

        pg_columns.append(f'    "{column_name}" {column_type}')

    pg_query += ",\n".join(pg_columns)

    # Eliminar la coma final después del paréntesis de cierre de la última columna, si existe
    if pg_columns:
        pg_columns[-1] = re.sub(r',\s*$', '', pg_columns[-1])

    pg_query += "\n);"
    
    # Eliminar la cadena "PRIMARY KEY" de la consulta
    pg_query = pg_query.replace('"DEFAULT" 0,',"")
    pg_query = pg_query.replace('"PRIMARY" KE', "")
    pg_query = pg_query.replace('"NOT" NULL,', "")
    pg_query = pg_query.replace('Y', "")

    

    return pg_query


def prepare_postgres_query(pg_query):
    if not isinstance(pg_query, str):
        raise ValueError("La consulta de PostgreSQL debe ser una cadena de texto.")

    # Buscar la última coma y eliminarla
    pg_query = re.sub(r',(\s*\n\);)$', r'\1', pg_query)

    # Agregar paréntesis de cierre en los tipos de datos NUMERIC
    pg_query = re.sub(r'NUMERIC\((\d+)(,\d+)?(\))?', r'NUMERIC(\1\2\3)', pg_query)

    return pg_query

def add_parenthesis(query):
    pattern = re.compile(r'"([^"]+)"\s+NUMERIC\([^\)]*\)')
    matches = re.findall(pattern, query)
    
    for column in matches:
        query = re.sub(rf'"{column}"\s+NUMERIC\([^\)]*\)', fr'"{column}" NUMERIC(10)', query)
    
    return query



# Nombre de la tabla en Oracle
table_name = 'table_name'

# Construir la cadena de conexión a Oracle
dsn = cx_Oracle.makedsn(oracle_host, oracle_port, sid=oracle_service_name)
oracle_connection = cx_Oracle.connect(user=oracle_username, password=oracle_password, dsn=dsn)

# Extraer la consulta de creación de la tabla de Oracle
oracle_query = extract_table_creation_query(oracle_connection, table_name)

# print(oracle_query)

pg_query = convert_query(oracle_query, postgres_schema, postgres_table)
prepared_query = prepare_postgres_query(pg_query)
final_query = add_parenthesis(prepared_query)

print(final_query)

