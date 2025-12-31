FROM apache/airflow:3.1.0

USER airflow

COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

WORKDIR /opt/airflow

RUN pip install --user -r requirements.txt
