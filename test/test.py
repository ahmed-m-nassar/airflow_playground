from airflow.utils.email import send_email

send_email(
    to="ahmed.nassar.cmp@gmail.com",
    subject="Test Airflow Email",
    html_content="This is a test from Airflow SMTP config"
)