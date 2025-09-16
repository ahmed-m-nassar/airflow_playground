from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
import random

# 1️⃣ Python functions for tasks
def start_task():
    print("Starting the workflow 🚀")

def task_a():
    print("Running Task A ✅")

def task_b():
    print("Running Task B ✅")

def decide_task():
    # Randomly choose which path to follow
    choice = random.choice(['task_a', 'task_b'])
    print(f"Branching to: {choice}")
    return choice

def final_task():
    print("Workflow finished! 🎉")

# 2️⃣ Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# 3️⃣ DAG definition
with DAG(
    'advanced_example_dag',
    default_args=default_args,
    description='A DAG with branching and multiple tasks',
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2025, 9, 16, 17, 40),  # fixed date in the future
    catchup=False,
    tags=['example', 'advanced'],
) as dag:

    # 4️⃣ Tasks
    start = PythonOperator(
        task_id='start',
        python_callable=start_task
    )

    branch = BranchPythonOperator(
        task_id='branching',
        python_callable=decide_task
    )

    task_a_op = PythonOperator(
        task_id='task_a',
        python_callable=task_a
    )

    task_b_op = PythonOperator(
        task_id='task_b',
        python_callable=task_b
    )

    end = PythonOperator(
        task_id='end',
        python_callable=final_task,
        trigger_rule='none_failed_or_skipped'  # ensures it runs after whichever branch executes
    )

    # 5️⃣ Set dependencies
    start >> branch
    branch >> task_a_op >> end
    branch >> task_b_op >> end
