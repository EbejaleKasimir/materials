from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator


default_args={
    'owner': 'ebejale',
    'retries':2,
    'retry_delay':timedelta(minutes=5)
    }


with DAG(
    dag_id='my_first_dag_v2',
    description='This is my first dag',
    start_date=datetime(2023,7,23), #july 23rd, by midnight
    schedule_interval=timedelta(days=1),
    default_args = default_args
    ) as dag:
   
    task1=BashOperator(
        task_id='greet_world',
        bash_command='echo hello world'
        )
   
    task2=BashOperator(
        task_id='greet_africa',
        bash_command='echo hello Africa!'
        )    
   
    task1.set_downstream(task2)
    #task2.set_upstream(task1)