FROM python:3.10-slim

# Install system deps
RUN apt-get update && apt-get install -y \
    build-essential default-libmysqlclient-dev libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

# Set Airflow version
ENV AIRFLOW_VERSION=2.7.2
ENV PYTHON_VERSION=3.10
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Install Airflow
RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Expose Airflow webserver port
EXPOSE 8080

# Start Airflow in standalone mode
CMD ["airflow", "standalone"]
