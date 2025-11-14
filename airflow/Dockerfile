FROM apache/airflow:2.6.3-python3.11

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres \
    apache-airflow-providers-amazon \
    "pandas==2.2.2" \
    "requests==2.32.3" \
    psycopg2-binary \