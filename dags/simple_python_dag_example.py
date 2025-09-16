from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

# Define Python functions for tasks
def say_hello():
    print("Hello, Airflow Students!")

def second_task():
    print("This is the second task")

# Define the DAG
with DAG(
    dag_id="simple_python_dag",
    start_date=datetime(2025, 9, 16),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    # Task 1: Print greeting
    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello
    )

    # Task 2: Second task
    task_second = PythonOperator(
        task_id="second_task",
        python_callable=second_task
    )

    # Set task dependencies
    task_hello >> task_second
