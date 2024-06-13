import psycopg2
import pandas as pd
from datetime import datetime

def extract_and_save_orders(working_directory):
    """
    This function connects to a PostgreSQL database, extracts data from the 'orders' table,
    and saves the data to both a CSV file and a JSON file.
    """
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

    # SQL query to extract data from the 'orders' table
    orders_query = "SELECT * FROM orders"

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
