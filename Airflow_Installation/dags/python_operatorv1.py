from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator


default_args={
    'owner': 'ebejale',
    'retries':2,
    'retry_delay':timedelta(minutes=5)
    }


def greet_world():
    print('Hello world!')




with DAG(
    dag_id='my_python_dag_v1',
    description='This is my first python dag',
    start_date=datetime(2023,7,26), #july 23rd, by midnight
    schedule_interval=timedelta(days=1),
    default_args = default_args
    ) as dag:
   
    task1=PythonOperator(
        task_id = 'greet',
        python_callable = greet_world
        )
   
    task1