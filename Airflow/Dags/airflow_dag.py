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
