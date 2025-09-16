from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator

# ------------------------------
# Data sources as arrays
# ------------------------------
source_a = ["row1", "row2", "row3"]  # 3 rows
source_b = ["row1", "row2", "row3"]          # 3 rows

# ------------------------------
# Python functions
# ------------------------------
def compare_counts():
    count_a = len(source_a)
    count_b = len(source_b)

    print(f"Source A count: {count_a}")
    print(f"Source B count: {count_b}")

    # Branch based on counts
    if count_a == count_b:
        return "send_success_email"
    else:
        return "send_failure_email"

# Email tasks
def success_email_content():
    return "<h3>✅ Data counts match! Reconciliation successful.</h3>"

def failure_email_content():
    return "<h3>❌ Data counts do NOT match! Reconciliation failed.</h3>"

# ------------------------------
# Define DAG
# ------------------------------
with DAG(
    dag_id="simple_branch_dag",
    start_date=datetime(2025, 9, 16),
    schedule_interval=None,
    catchup=False,
    tags=["data_engineering", "tutorial"],
) as dag:


    # Task: Compare counts and decide branch
    branch = BranchPythonOperator(
        task_id="compare_counts",
        python_callable=compare_counts
    )

    # Task: Send success email
    send_success_email = EmailOperator(
        task_id="send_success_email",
        to="recipient@example.com",  # Mailtrap inbox
        subject="Data Reconciliation Success",
        html_content=success_email_content(),
    )

    # Task: Send failure email
    send_failure_email = EmailOperator(
        task_id="send_failure_email",
        to="recipient@example.com",  # Mailtrap inbox
        subject="Data Reconciliation Failure",
        html_content=failure_email_content(),
    )

  
    # ------------------------------
    # Set dependencies
    # ------------------------------
    branch >> [send_success_email, send_failure_email]
