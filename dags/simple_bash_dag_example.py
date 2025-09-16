from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define the DAG
with DAG(
    dag_id="simple_bash_dag",
    start_date=datetime(2025, 9, 16),  # Set the start date
    schedule_interval="@daily",         # Run daily
    catchup=False,                      # Don't run past dates
    tags=["example"],
) as dag:

    # Task 1: Print a greeting
    say_hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello, Airflow Students!'"
    )

    # Task 2: Print This is the second task
    second_task = BashOperator(
        task_id="list_files",
        bash_command="echo 'This is the second task!'"
    )

    # Define task dependencies
    say_hello >> second_task
