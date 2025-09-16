from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import os

# Paths
DATA_DIR = "/workspaces/airflow_playground/dags/data"
RAW_FILE = os.path.join(DATA_DIR, "titanic_raw.csv")
CLEAN_FILE = os.path.join(DATA_DIR, "titanic_clean.csv")

os.makedirs(DATA_DIR, exist_ok=True)

# DAG default args
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# Extract step
def extract_data():
    url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
    r = requests.get(url)
    with open(RAW_FILE, "wb") as f:
        f.write(r.content)
    print(f"Saved raw data to {RAW_FILE}")

# Transform step
def transform_data():
    df = pd.read_csv(RAW_FILE)
    # Drop rows with null Age
    df = df.dropna(subset=["Age"])
    # Filter: only survivors
    df = df[df["Survived"] == 1]
    # Add computed column: Age bucket
    df["AgeGroup"] = pd.cut(df["Age"], bins=[0,12,18,40,60,80], labels=["Child","Teen","Adult","MidAge","Senior"])
    df.to_csv(CLEAN_FILE, index=False)
    print(f"Saved cleaned data to {CLEAN_FILE}")

# Validate step
def validate_data():
    df = pd.read_csv(CLEAN_FILE)
    # Checks
    assert df.shape[0] > 0, "No rows after cleaning!"
    assert df["Age"].isnull().sum() == 0, "Null values in Age!"
    assert df.duplicated().sum() == 0, "Duplicate rows found!"
    print("Validation passed!")



# Build DAG
with DAG(
    dag_id="advanced_example_dag_2",
    default_args=default_args,
    description="Mini ETL pipeline with extract, transform, load, validate",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    # transform = PythonOperator(
    #     task_id="transform_data",
    #     python_callable=transform_data
    # )

    # validate = PythonOperator(
    #     task_id="validate_data",
    #     python_callable=validate_data
    # )

    # Dependencies
    extract
    # extract >> transform >> validate >> load_to_postgres
