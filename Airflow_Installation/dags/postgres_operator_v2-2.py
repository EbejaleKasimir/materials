from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'Byron',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='my_postgres_dag_v3-1',
    description='This is my first postgres dag',
    start_date=datetime(2023, 7, 26),
    schedule_interval=timedelta(days=1),
    default_args=default_args,
) as dag:

    task1 = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='my_connection',
        sql='CREATE TABLE IF NOT EXISTS airflow_users (id SERIAL PRIMARY KEY, name VARCHAR(30))',
    )

    task2 = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='my_connection',
        sql='INSERT INTO airflow_users (name) VALUES ("Chidi", "Joseph", "Cindy", "Sarai")',
    )

    task1 >> task2

