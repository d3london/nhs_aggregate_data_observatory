import sys
import duckdb
import yaml
import os

def generate_sources_yml(database_path, schema_name, file_name):

    con = duckdb.connect(database=database_path, read_only=True)

    query = f"SELECT table_name FROM information_schema.tables WHERE table_schema='{schema_name}'"
    tables = con.execute(query).fetchall()

    con.close()

    # prepare data for schema.yml file
    data = {
        'version': 2,
        'sources': [
            {
                'name': schema_name,
                'schema': schema_name,
                'tables': [{'name': table[0], 'description': table[0]} for table in tables]
            }
        ]
    }
    sources_yml_file_path = os.path.join('models', 'staging', file_name)

    # check if the sources.yml file already exists and warn about overwriting
    if os.path.exists(sources_yml_file_path):
        response = input(f"Warning: {sources_yml_file_path} will be overwritten with new content. Continue? (y/n): ")
        if response.lower() != 'y':
            print("Operation terminated")
            return
    
    # write to sources.yml
    with open(sources_yml_file_path, 'w') as file:
        yaml.dump(data, file, default_flow_style=False, sort_keys=False)

    print(f"sources.yml has been updated with {len(tables)} tables at {sources_yml_file_path}.")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python generate_sources_yml.py <path_to_database> <schema_name> <file_name>")
        sys.exit(1)

    db_path, schema_name, file_name = sys.argv[1], sys.argv[2], sys.argv[3]

    generate_sources_yml(db_path, schema_name, file_name)