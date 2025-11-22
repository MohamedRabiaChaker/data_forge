FROM apache/airflow:slim-3.1.0

COPY requirements.txt /opt/airflow/requirements.txt

WORKDIR /opt/airflow

RUN pip install -r requirements.txt 

USER 50000
