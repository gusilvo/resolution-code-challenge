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


# Dictionary for tap-postgres catalogs
postgres_tables = [
    {'table_name': 'categories',
     'catalog_path': r'C:\Users\User\Meltano\my-project\extract\tap-postgres\public-categories.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'customer_customer_demo',
     'catalog_path': r'C:\Users\User\Meltano\my-project\extract\tap-postgres\public-customer_customer_demo.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'customer_demographics',
     'catalog_path': r'C:\Users\User\Meltano\my-project\extract\tap-postgres\public-customer_demographics.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'customers',
     'catalog_path': r'C:\Users\User\Meltano\my-project\extract\tap-postgres\public-customers.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'employee_territories',
     'catalog_path': r'C:\Users\User\Meltano\my-project\extract\tap-postgres\public-employee_territories.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'employees',
     'catalog_path': r'C:\Users\User\Meltano\my-project\extract\tap-postgres\public-employees.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'orders',
     'catalog_path': r'C:\Users\User\Meltano\my-project\extract\tap-postgres\public-orders.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'products',
     'catalog_path': r'C:\Users\User\Meltano\my-project\extract\tap-postgres\public-products.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'region',
     'catalog_path': r'C:\Users\User\Meltano\my-project\extract\tap-postgres\public-region.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'shippers',
     'catalog_path': r'C:\Users\User\Meltano\my-project\extract\tap-postgres\public-shippers.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'suppliers',
     'catalog_path': r'C:\Users\User\Meltano\my-project\extract\tap-postgres\public-suppliers.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'territories',
     'catalog_path': r'C:\Users\User\Meltano\my-project\extract\tap-postgres\public-territories.catalog.json',
     'base_output_path': r'output/data/tap-postgres'},
    {'table_name': 'us_states',
     'catalog_path': r'C:\Users\User\Meltano\my-project\extract\tap-postgres\public-us_states.catalog.json',
     'base_output_path': r'output/data/tap-postgres'}
]

# Dictionary for tap-csv files definition path
csv_table = {'csv_files_definition_path': r'C:\Users\User\Meltano\my-project\extract\tap-csv\first-extract\orders_details_files.json',
             'base_output_path': r'output\data\tap-csv'}


# Call the extract_and_load functions with the defined tables and the specific working directory
if __name__ == "__main__":
    meltano_directory = r'C:\Users\User\Meltano\my-project'
    extract_and_load_postgres(postgres_tables, meltano_directory)
    extract_and_load_csv(csv_table, meltano_directory)
