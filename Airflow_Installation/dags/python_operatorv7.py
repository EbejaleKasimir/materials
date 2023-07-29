from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
#from pipeline import my_name


default_args={
    'owner': 'Byron',
    'retries':2,
    'retry_delay':timedelta(minutes=5)
    }


def greet_them(ti):
        location = ti.xcom_pull(task_ids='my_address',key='location')
        name = ti.xcom_pull(task_ids='my_address',key='name')
        age = ti.xcom_pull(task_ids='my_address',key='age')
       
        print(f'my name is {name} and I am {age}'
          f'years old and I am from {location} ')


def my_address(ti):
    ti.xcom_push(key='name',value='Charles')
    ti.xcom_push(key='age',value=32)
    ti.xcom_push(key='location',value='Lagos')


with DAG(
    dag_id='my_python_dag_v7',
    description='This is my first python dag',
    start_date=datetime(2023,7,26,2),
    schedule_interval=timedelta(days=1),
    default_args = default_args
    ) as dag:
   
    task1=PythonOperator(
        task_id = 'greet',
        python_callable = greet_them,
        )
   
    # task2=PythonOperator(
    #     task_id='name',
    #     python_callable=my_name
       
    # )
   
    task3 = PythonOperator(
        task_id = 'my_address',
        python_callable=my_address
       
    )
   
    task3 >> task1
