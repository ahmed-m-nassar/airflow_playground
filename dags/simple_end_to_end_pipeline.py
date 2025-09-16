from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from cleaned_etl.bronze import load_bronze_data
from cleaned_etl.silver import clean_silver_data
from cleaned_etl.gold import aggregate_gold_data
import os

# ------------------------------
# Define Python functions for tasks
# ------------------------------
def bronze_layer():
    load_bronze_data()

def silver_layer():
    clean_silver_data()

def gold_layer():
    aggregate_gold_data()

# ------------------------------
# Define the DAG
# ------------------------------
with DAG(
    dag_id="simple_end_to_end_pipeline_dag",
    start_date=datetime(2025, 9, 16),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    # Tasks
    bronze_task = PythonOperator(
        task_id="bronze_layer_task",
        python_callable=bronze_layer
    )

    silver_task = PythonOperator(
        task_id="silver_layer_task",
        python_callable=silver_layer
    )

    gold_task = PythonOperator(
        task_id="gold_layer_task",
        python_callable=gold_layer
    )

    # Set task dependencies
    bronze_task >> silver_task >> gold_task
