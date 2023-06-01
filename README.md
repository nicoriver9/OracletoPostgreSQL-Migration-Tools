# Oracle to PostgreSQL Migration Tools

This repository contains a set of functions to facilitate the migration of data and schemas from Oracle to PostgreSQL.

## Available Functions

### 1. Extraction of data from Oracle and loading into PostgreSQL

- `extract_and_load_data`: This function allows you to extract data from an Oracle table using SELECT queries and load it into an equivalent table in PostgreSQL.

### 2. Extraction and creation of sequences from Oracle to PostgreSQL

- `extract_and_create_sequences`: This function allows you to extract sequences from Oracle and create them in PostgreSQL, assigning them the corresponding current value.

### 3. Conversion of Oracle table creation query to PostgreSQL

- `convert_query`: This function converts an Oracle table creation query to its equivalent in PostgreSQL. It adjusts data types and adds the schema and table name in PostgreSQL.

## How to Use the Functions

1. Make sure you have the necessary drivers and libraries installed to connect to both Oracle and PostgreSQL.

2. Import the required functions in your migration script:

    ```python
    from oracle_to_postgres import extract_and_load_data, extract_and_create_sequences, convert_query
    ```

3. Use the functions as per your needs. Below are examples of usage:

    - Example of data extraction and loading into PostgreSQL:

      ```python
      oracle_table = 'oracle_table_name'
      postgres_table = 'postgres_table_name'

      extract_and_load_data(oracle_table, postgres_table)
      ```

    - Example of extraction and creation of sequences in PostgreSQL:

      ```python
      extract_and_create_sequences()
      ```

    - Example of conversion of table creation query:

      ```python
      oracle_query = '... oracle_table_creation_query ...'
      postgres_schema = 'target_schema'
      postgres_table = 'target_table_name'

      pg_query = convert_query(oracle_query, postgres_schema, postgres_table)
      ```

## Contributions

This project was developed with the assistance of ChatGPT, an AI language generation tool based on OpenAI's GPT-3.5. We appreciate its help in creating the functions and resolving issues.

Feel free to contribute to the project, make improvements, or provide feedback. Your contributions are welcome!

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.
