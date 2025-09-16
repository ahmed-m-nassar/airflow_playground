from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email




default_args = {
    "owner": "data_eng",
    "depends_on_past": False,
    "email": ["ahmed.danger85@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "email_on_success": True,  # now it will email on success
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def send_success_email(**context):
    send_email(
        to="ahmed.nassar.cmp@gmail.com",
        subject="Random Email",
        html_content="<h3>This is a random email</h3><p>random</p>"
    )



with DAG(
    dag_id="etl_project",
    default_args=default_args,
    description="ETL pipeline called via BashOperator",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["etl", "bash"],
) as dag:

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command="python /workspaces/airflow_playground/clean_script/main.py",
    )

    send_email_task = PythonOperator(
    task_id="send_email_task",
    python_callable=send_success_email,
    provide_context=True,
    )

    test_failure = BashOperator(
    task_id="test_failure",
    bash_command="exit 1",  # exit code 1 forces failure
    )
    run_pipeline >> send_email_task >> test_failure