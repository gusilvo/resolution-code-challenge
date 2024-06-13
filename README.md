# RESOLUTION FOR THE INCIDIUM CODE CHALLENGE
_by Gustavo Silva_

## Resolution

### Preparing the environment:

First, I installed Meltano using 'pip install meltano' and then created a directory named meltano to manage all the programs.

With all basic requirements configured, I customized Meltano for my specific needs by installing the tap-postgres, tap-csv, target-csv, and target-postgres with the following commands:

```
meltano add extractor tap-postgres

meltano add extractor tap-csv

meltano add loader target-csv

meltano add loader target-postgres
```

Tap-postgres and tap-csv were used to extract the initial data as required, and target-csv was used to load that data onto the local disk.

I specifically chose target-csv instead of target-jsonl because there is no dedicated tap-jsonl available. After extracting the data to the local disk, it was then loaded into the database.

For the initial extraction, it was necessary to configure the tap-postgres extractor with the following command:

```
meltano config tap-postgres set sqlalchemy_url postgresql:northwind_user
@localhost:5432/northwind
```

The tap-csv configuration will be handled in the Airflow script when the data needs to be extracted from the local disk to another database.

Similarly, target-csv will be configured later since tap-postgres extracts one table at a time.

Lastly, target-postgres can be configured early with the following command:

```
meltano config target-postgres set sqlalchemy_url postgresql:new_northwind_user
@localhost:5432/new_northwind
```

In the end, the environment was ready, but due to a bug where the unit_price type in products and freight in orders was not recognized, it was necessary to create custom catalog for each table in the Northwind database. Specifically, for products and orders, I changed the type from real to number, as it is compatible with JSON files.

The catalogs can be found in the extract/tap-postgres directory.

Lastly, CSV files were defined for each table for Step 2.

Now, with the environment fully prepared, it was time to execute the tasks for Airflow

### Creating Airflow Tasks:

The first task extracts the tables and saves them to the local disk:

```python
import subprocess
import os
from datetime import datetime


def extract_and_load_postgres(tables, meltano_directory):

    # Change for the Meltano directory
    os.chdir(meltano_directory)

    for table in tables:
        table_name = table['table_name']
        catalog_path = table['catalog_path']
        base_output_path = table['base_output_path']

        # Set up the catalog for tap-postgres
        subprocess.run(['meltano', 'config', 'tap-postgres', 'set', '_catalog', catalog_path])

        # Set the destination path for the target-csv based on today's date
        today_date = datetime.now().strftime('%Y-%m-%d')
        path_to_destination_folder = f'{base_output_path}/{table_name}/{today_date}'

        # Create the folder based on the date if it doesn't already exist
        os.makedirs(path_to_destination_folder, exist_ok=True)

        # Set the destination path for the target-csv
        subprocess.run(['meltano', 'config', 'target-csv', 'set', 'destination_path', path_to_destination_folder])

        # Run the tap-postgres to target-csv pipeline
        subprocess.run(['meltano', 'run', 'tap-postgres', 'target-csv'])


def extract_and_load_csv(table, meltano_directory):

    # Change for the meltano directory
    os.chdir(meltano_directory)

    csv_files_definition_path = table['csv_files_definition_path']
    base_output_path = table['base_output_path']

    # Set up the catalog for tap-csv
    subprocess.run(['meltano', 'config', 'tap-csv', 'set', 'csv_files_definition', csv_files_definition_path])

    # Set the destination path for the target CSV based on today's date
    today_date = datetime.now().strftime('%Y-%m-%d')
    path_to_destination_folder = f'{base_output_path}/{today_date}'

    # Create the folder based on the date, if it doesn't already exist
    os.makedirs(path_to_destination_folder, exist_ok=True)

    # Set the destination path for the target CSV
    subprocess.run(['meltano', 'config', 'target-csv', 'set', 'destination_path', path_to_destination_folder])

    # Run the pipeline tap-postgres target-csv
    subprocess.run(['meltano', 'run', 'tap-csv', 'target-csv'])

# Define the working directory
meltano_directory = r'C:\Users\User\Meltano\'

# Dictionary for tap-postgres catalogs
postgres_tables = [
    {'table_name': 'categories',
     'catalog_path': rf'{meltano_directory}\extract\tap-postgres\public-categories.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'customer_customer_demo',
     'catalog_path': rf'{meltano_directory}\extract\tap-postgres\public-customer_customer_demo.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'customer_demographics',
     'catalog_path': rf'{meltano_directory}\extract\tap-postgres\public-customer_demographics.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'customers',
     'catalog_path': rf'{meltano_directory}\extract\tap-postgres\public-customers.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'employee_territories',
     'catalog_path': rf'{meltano_directory}\extract\tap-postgres\public-employee_territories.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'employees',
     'catalog_path': rf'{meltano_directory}\my-project\extract\tap-postgres\public-employees.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'orders',
     'catalog_path': rf'{meltano_directory}\extract\tap-postgres\public-orders.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'products',
     'catalog_path': rf'{meltano_directory}\extract\tap-postgres\public-products.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'region',
     'catalog_path': rf'{meltano_directory}\extract\tap-postgres\public-region.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'shippers',
     'catalog_path': rf'{meltano_directory}\my-project\extract\tap-postgres\public-shippers.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'suppliers',
     'catalog_path': rf'{meltano_directory}\extract\tap-postgres\public-suppliers.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'territories',
     'catalog_path': rf'{meltano_directory}\extract\tap-postgres\public-territories.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'us_states',
     'catalog_path': rf'{meltano_directory}\extract\tap-postgres\public-us_states.catalog.json',
     'base_output_path': r'output/data/tap-postgres'}
]

# Dictionary for tap-csv files definition path
csv_table = {'csv_files_definition_path': rf'{meltano_directory}\extract\tap-csv\first-extract\order_details_files.json',
             'base_output_path': r'output\data\tap-csv'}


# Call the extract_and_load functions with the defined tables and the specific working directory
if __name__ == "__main__":
    extract_and_load_postgres(postgres_tables, meltano_directory)
    extract_and_load_csv(csv_table, meltano_directory)
```

The second task executes the second step and loads the data into the new database:
```python
from csv_path_editor import replace_path_in_json
import os
import subprocess
from datetime import datetime


# Function to configure Meltano with updated paths and Extract and Load to the Database
def extract_and_load_final(meltano_directory):

    # Change for the Meltano directory
    os.chdir(meltano_directory)

    for table in tables:
        # Update JSON with new path based on table name
        today_date = datetime.now().strftime('%Y-%m-%d')
        new_path = f'output/data/tap-postgres/{table["table_name"]}/{today_date}'
        replace_path_in_json(table['file_path'], new_path)

        # Example command to configure tap-csv with Meltano
        subprocess.run(['meltano', 'config', 'tap-csv', 'set', 'csv_files_definition', table['file_path']])

        # Run Meltano pipeline tap-csv target-postgres
        subprocess.run(['meltano', 'run', 'tap-csv', 'target-postgres'])

# Define the working directory
meltano_directory = r'C:\Users\User\Meltano\'

tables = tables_files = [
    {'table_name': 'categories',
     'file_path': rf'{meltano directory}\extract\tap-csv\final_extract\categories_file.json'},
    {'table_name': 'customer_customer_demo',
     'file_path': rf'{meltano directory}\extract\tap-csv\final_extract\customer_customer_demo_file.json'},
    {'table_name': 'customer_demographics',
     'file_path': rf'{meltano directory}\extract\tap-csv\final_extract\customer_demographics_file.json'},
    {'table_name': 'customers',
     'file_path': rf'{meltano directory}\extract\tap-csv\final_extract\customers_file.json'},
    {'table_name': 'employee_territories',
     'file_path': rf'{meltano directory}\extract\tap-csv\final_extract\employee_territories_file.json'},
    {'table_name': 'employees',
     'file_path': rf'{meltano directory}\extract\tap-csv\final_extract\employees_file.json'},
    {'table_name': 'orders',
     'file_path': rf'{meltano directory}\extract\tap-csv\final_extract\orders_file.json'},
    {'table_name': 'products',
     'file_path': rf'{meltano directory}\extract\tap-csv\final_extract\products_file.json'},
    {'table_name': 'region',
     'file_path': rf'{meltano directory}\extract\tap-csv\final_extract\region_file.json'},
    {'table_name': 'shippers',
     'file_path': rf'{meltano directory}\extract\tap-csv\final_extract\shippers_file.json'},
    {'table_name': 'suppliers',
     'file_path': rf'{meltano directory}\extract\tap-csv\final_extract\suppliers_file.json'},
    {'table_name': 'territories',
     'file_path': rf'{meltano directory}\extract\tap-csv\final_extract\territories_file.json'},
    {'table_name': 'us_states',
     'file_path': rf'{meltano directory}t\extract\tap-csv\final_extract\us_states_file.json'},
    {'table_name': 'order_details',
     'file_path': rf'{meltano directory}\extract\tap-csv\final_extract\order_details_file.json'}
]

# Call function to configure Meltano and execute pipeline
if __name__ == "__main__":
    extract_and_load_final(meltano_directory)
```
To do that, was necessary make a destination file rewriter:
```python
import json


def replace_path_in_json(file_path, new_path):
    try:
        # Load JSON from the existing file
        with open(file_path, 'r') as file:
            data = json.load(file)

        # Replace the value of "path" with the new path
        data["path"] = new_path

        # Save the updated JSON back to the file
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=2)

        print(f'Successfully replaced the value of "path" in JSON file "{file_path}".')

    except FileNotFoundError:
        print(f'JSON file "{file_path}" not found.')

    except Exception as e:
        print(f'An error occurred while replacing the value of "path" in JSON file "{file_path}": {e}')
```

Finally, the third step creates a file as required:
```
import psycopg2
import pandas as pd
from datetime import datetime

def extract_and_save_orders(working_directory):

    # Database connection parameters
    db_params = {
        'dbname': 'new_northwind',
        'user': 'new_northwind_user',
        'password': 'new_thewindisblowing',
        'host': 'localhost',
        'port': '5432'
    }

    # Connect to PostgreSQL database
    try:
        conn = psycopg2.connect(**db_params)
        print("Database connection successful")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # SQL query to join 'orders' and 'order_details' tables
    orders_query = """
    SELECT o.*, od.*
    FROM orders o
    JOIN order_details od ON o.order_id = od.order_id
    """

    # Load data from 'orders' table into a pandas DataFrame
    try:
        orders_df = pd.read_sql_query(orders_query, conn)
        print("Data extraction successful")
    except Exception as e:
        print(f"Error executing query: {e}")
        conn.close()
        return

    # Close the database connection
    conn.close()

    # Get the current date
    current_date = datetime.now().strftime('%Y-%m-%d')

    # File paths to save the data
    output_csv_path = rf'{working_directory}\Query\orders_details_{current_date}.csv'

    # Save DataFrame to CSV file
    try:
        orders_df.to_csv(output_csv_path, index=False)
        print(f"Data successfully saved to {output_csv_path}")
    except Exception as e:
        print(f"Error saving data to CSV: {e}")


# Define the working directory
meltano_directory = r'C:\Users\User\Meltano\'


if __name__ == "__main__":
    extract_and_save_orders(working_directory)
```

Doing all that, was needed to unit the Steps on a Dag:
```python
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import subprocess
import logging

# Define the function to be called by PythonOperator
def run_step(step):
    logging.info(f'Starting step execution: {step}')
    subprocess.run(['python', f'{step}'])
    logging.info(f'Finished step execution: {step}')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'Extract_and_Load',
    default_args=default_args,
    description='Run my_script.py daily',
    schedule_interval='@daily',
)

# Define the Step1 using PythonOperator
Step1 = PythonOperator(
    task_id='Extract_and_Load_Step1',
    python_callable=run_step,
    op_args=[r'path-to\Airflow\Steps_and_Others\Step1.py'],
    dag=dag,
)

# Define the Step2 using PythonOperator
Step2 = PythonOperator(
    task_id='Extract_and_Load_Step2',
    python_callable=run_step,
    op_args=[r'path-to\Airflow\Steps_and_Others\Step2.py'],
    dag=dag,
)

# Define the Step3 using PythonOperator
Step3 = PythonOperator(
    task_id='Extract_and_Load_Step3',
    python_callable=run_step,
    op_args=[r'path-to\Airflow\Steps_and_Others\Step3.py'],
    dag=dag,
)

# DEFINE DEPENDENCY
Step1 >> Step2 >> Step3
```

## How To Implement:

Make sure you have all the basic requirements installed such as Docker, psycopg2, pandas, Meltano, and Airflow.

You will need to edit some files for the script to work properly:

Step1.py, Step2.py, Step3.py, airflow_dag, and \first-extract\order_details_file.json

1. In Step1.py, Step2.py, and Step3.py, change the variable working_directory to Meltano's path.

2. In airflow_dag, change the initial path in op_args of each step.

3. In \first-extract\order_details_file.json, complete the path to the data.

After editing, you can run the Docker containers:

```
docker-compose up -d
```

Lastly, access Airflow and trigger an execution:

```
docker exec -it airflow-container airflow dags trigger Extract_and_Load
```

Your query will be in Meltano/Query/
