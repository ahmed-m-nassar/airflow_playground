from datetime import datetime
from airflow import DAG
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="simple_email_dag",
    start_date=datetime(2025, 9, 16),
    schedule_interval=None,  # Run manually
    catchup=False,
) as dag:

    send_email = EmailOperator(
        task_id="simple_email_dag",
        to="ahmed.nassar.cmp@gmail.com",  # Can be any email
        subject="Airflow Mailtrap Test",
        html_content="<h3>Hello from Airflow via Mailtrap!</h3>",
    )
