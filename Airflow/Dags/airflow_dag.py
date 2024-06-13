from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


# Define the function to be called by PythonOperator
def run_step(step):
    import subprocess
    subprocess.run(['python', f'{step}'])


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
    op_args=[r'C:\Users\User\Steps\Step1.py'],
    dag=dag,
)

# Define the Step2 using PythonOperator
Step2 = PythonOperator(
    task_id='Extract_and_Load_Step2',
    python_callable=run_step,
    op_args=[r'C:\Users\User\Steps\Step2.py'],
    dag=dag,
)

# DEFINES DEPENDECY
task1 >> task2

