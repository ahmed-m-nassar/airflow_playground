from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from project.bronze_layer import load_bronze_layer
from project.silver_layer import load_silver_layer
from project.gold_layer import load_gold_layer
from datetime import datetime, timedelta
import pandas as pd
import requests
import os


# DAG default args
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}





# Build DAG
with DAG(
    dag_id="end_to_end_project",
    default_args=default_args,
    description="Mini ETL pipeline with extract, transform, load, validate",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    bronze_task = PythonOperator(
        task_id="bronze_etl",
        python_callable=load_bronze_layer
    )
    silver_task = PythonOperator(
        task_id="silver_etl",
        python_callable=load_silver_layer
    )

    gold_task = PythonOperator(
        task_id="gold_etl",
        python_callable=load_gold_layer
    )

    bronze_task >> silver_task >> gold_task