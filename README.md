### Getting Started With DBT

(1) This project uses:
dbt-core==1.7.11
dbt-duckdb==1.7.3
duckdb==0.10.1

(2) To initialise a project, run:
```
dbt init my_project
```
within the project folder. This will set the default folder structure and empty configuration files. 

(3) DBT uses a file called profiles.yml for connection setups. This file is usually located in the home directory under .dbt/. Edit the profiles.yml to match your database credentials and connection settings. An example profile for this project is shown below:
```                                                                                             
nhs_aggregate_data_observatory:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: nhs_agg_data.duckdb
      threads: 4
```
This file also holds database credentials. An template profile for a postgres instance is shown below:
```
my_project:
  target: dev
  outputs:
    dev:
      type: postgres
      threads: 4
      host: your_remote_host_address
      port: 5432
      user: your_username
      pass: your_password
      dbname: your_database_name
      schema: your_schema_name
```

(4) To test the database connection, run:
```
dbt debug
```

(5) As models are created, the pipeline can be compiled (to test logic) and then built using:
```
dbt compile
dbt build
```
### Data Loading
load_data.py will parse and load all CSV files within a directory, as tables in a specified schema in duckdb.
```
python load_data.py <database_path> <data_directory> <schema_name> <sample_chars>
```
generate_sources_config.py will then create a sources file for the start of a dbt pipeline.

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
