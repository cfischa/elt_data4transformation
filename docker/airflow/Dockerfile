FROM apache/airflow:2.9.1-python3.11

USER root

# System packages required for Airflow connector dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
    libpq-dev \
    python3-dev \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir --upgrade pip setuptools wheel

COPY requirements.txt /opt/airflow/requirements-custom.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements-custom.txt
