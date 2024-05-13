import os
import sys
import duckdb
import pandas as pd
import clevercsv

"""
## Introduction
    Data-loader that loads all CSV files from given directory into a specified schema in a DuckDB database;
    CleverCSV is used as a method for robust CSV loading by sampling n chars to detect dialect/encoding before passing to pandas;
    If the table already exists, it will be skipped;
    Each CSV file is loaded into a table named after the CSV file;
    
## Usage
    python load_data.py <database_path> <data_directory> <schema_name> <sample_chars>

## Arguments
    database_path: path to the DuckDB database file
    data_directory: path to directory containing CSV files (including within subdir structure)
    schema_name: name of schema under which all tables are loaded. If the schema does not exist, it will be created
    sample_chars: number of characters for clevercsv to sample for infering syntax and encoding (leave empty to sample entire CSVs) 

## Requirements 
    pandas, duckdb, clevercsv

## Assumptions
    CSV files are properly formatted and compatible with being loaded directly into pandas DataFramem, CSVs that fail to load are presented for review
    CSVs must not be updated with new data in the future, as additional data will be not be loaded
    CSVs should not be renamed after being loaded, otherwise they will be loaded in duplicate

This script is intended for use as a standalone, robust, multi-CSV loader prior to running DBT for further transformation.
"""

def load_csvs_to_duckdb(database_path, data_directory, schema_name, sample_chars):
    
    problematic_files = []  # list to store CSVs with fatal errors
    problematic_tables = [] # list to store files that cannot be loaded as table

    if data_directory is None:
        data_directory = os.path.dirname(os.path.realpath(__file__))
    
    print(f"Executing script to load csvs from: {data_directory}")

    # db connection
    print("Connecting to database...")
    con = duckdb.connect(database=database_path, read_only=False)

    con.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')

    # confirm schema creation and exit if unable to locate 
    schema_exists = con.execute(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}'").fetchone()
    if not schema_exists:
        print(f"Error: Schema '{schema_name}' could not be created or confirmed.")
        exit()
    else:
        print(f"Schema '{schema_name}' confirmed.")

    # walk through subdir in data directory
    for subdir, dirs, files in os.walk(data_directory):
        for filename in files:
            if filename.endswith('.csv'):
                
                print(f"Found CSV file: {filename}")

                # get path
                file_path = os.path.join(subdir, filename)

                # use filename without extension as the table name
                table_name = os.path.splitext(filename)[0].replace('-', '_').replace(' ', '_')

                # check if the table already exists
                if con.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'").fetchone()[0] > 0:
                    print(f"Table '{table_name}' already exists.")
                    continue

                # if does not exist, try CSV read using CleverCSV
                try:
                    df = clevercsv.read_dataframe(file_path, num_chars=sample_chars, dtype=object)
                except Exception as e:
                    print(f"Failed to load {file_path} due to: {e}")
                    problematic_files.append(f"{file_path}: {e}")
                    continue

                # load table from dataframe
                try:
                    create_query = f"CREATE TABLE {schema_name}.{table_name} AS SELECT * FROM df"
                    con.execute(create_query)

                    # verify new table
                    df_row_count = len(df)
                    row_count_query = f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
                    table_row_count = con.execute(row_count_query).fetchone()[0]
                    print(f"Loaded table '{table_name}' with {table_row_count} rows from CSV with {df_row_count} rows.")
                except Exception as e:
                    print(f"Error creating table {table_name} from {file_path}: {e}")
                    problematic_tables.append(f"{table_name} from {file_path}: {e}")

    con.close()

    if problematic_files:
        print("CSVs with load errors that require manual review:")
        for file in problematic_files:
            print(file)

    if problematic_tables:
        print("Unable to load following tables:")
        for table in problematic_tables:
            print(table)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python load_data.py <database_path> <data_directory> <schema_name> <sample_chars>")
        sys.exit(1)

    database_path = sys.argv[1]
    data_directory = sys.argv[2]
    schema_name = sys.argv[3]

    # default sample_chars to None if nothing given
    sample_chars = None if len(sys.argv) <= 4 else sys.argv[4]

    # Convert sample_chars to int if it is not None
    if sample_chars is not None:
        try:
            sample_chars = int(sample_chars)
        except ValueError:
            print("Error: sample_chars must be an integer or left blank for sampling of entire document")
            sys.exit(1)

    load_csvs_to_duckdb(database_path, data_directory, schema_name, sample_chars)
