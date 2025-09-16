"""
Airflow Playground DAG

This DAG is designed for students to safely experiment with:
- PythonOperator
- BashOperator
- EmailOperator (Mailtrap recommended for testing)
- Task dependencies
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

# ------------------------------
# Example Python functions
# ------------------------------
def my_python_task():
    """
    Example Python task.
    Students can modify this to print messages or perform simple calculations.
    """
    print("Hello from PythonOperator! Modify me to try new things.")


def add_numbers():
    a = 5
    b = 10
    result = a + b
    print(f"The sum of {a} + {b} is {result}")


# ------------------------------
# Define the DAG
# ------------------------------
with DAG(
    dag_id="simple_playground_dag_example",
    start_date=datetime(2025, 9, 16),
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=["playground"],
) as dag:

    # ------------------------------
    # PythonOperator tasks
    # ------------------------------
    python_task_1 = PythonOperator(
        task_id="python_task_1",
        python_callable=my_python_task,
    )

    python_task_2 = PythonOperator(
        task_id="python_task_2",
        python_callable=add_numbers,
    )

    # ------------------------------
    # BashOperator task
    # ------------------------------
    bash_task = BashOperator(
        task_id="bash_task",
        bash_command='echo "Hello from BashOperator! Modify this command."',
    )

    # ------------------------------
    # EmailOperator task (Mailtrap recommended)
    # ------------------------------
    email_task = EmailOperator(
        task_id="email_task",
        to="recipient@example.com",  # For testing, Mailtrap inbox will receive it
        subject="Playground DAG Email",
        html_content="<h3>This is a test email from Airflow Playground DAG.</h3>",
    )

    # ------------------------------
    # Set task dependencies
    # ------------------------------
    # Students can modify these dependencies to experiment
    [python_task_1, python_task_2] >> bash_task >> email_task
