# Dockerfile
FROM apache/airflow:2.5.1-python3.9

USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean

USER airflow
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
COPY config/ /opt/airflow/config/