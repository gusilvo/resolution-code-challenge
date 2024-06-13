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
