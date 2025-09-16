from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Python function for the first task
def greet():
    print("Hello from PythonOperator!")

# Define the DAG
with DAG(
    dag_id="python_bash_dag",
    start_date=datetime(2025, 9, 16),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    # Task 1: PythonOperator
    python_task = PythonOperator(
        task_id="python_greet",
        python_callable=greet
    )

    # Task 2: BashOperator
    bash_task = BashOperator(
        task_id="bash_hello",
        bash_command='echo "Hello from BashOperator!"'
    )

    # Set task dependencies
    python_task >> bash_task
