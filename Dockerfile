FROM apache/airflow:slim-2.10.0

COPY requirements.txt /opt/airflow/reqirements.txt 

WORKDIR /opt/airflow

RUN pip install -r reqirements.txt 

USER 50000
