from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from pipeline import my_name


default_args={
    'owner': 'ebejale',
    'retries':2,
    'retry_delay':timedelta(minutes=5)
    }


def greet_them(name, age, ti):
        location = ti.xcom_pull(task_ids='my_address')
        print(f'my name is {name} and I am {age}'
          f'years old and I am from {location} ')


def my_address():
    return 'Lagos'


with DAG(
    dag_id='my_python_dag_v5',
    description='This is my first python dag',
    start_date=datetime(2023,7,26,2),
    schedule_interval=timedelta(days=1),
    default_args = default_args
    ) as dag:
   
    task1=PythonOperator(
        task_id = 'greet',
        python_callable = greet_them,
        op_kwargs = {'name':'Charles','age':32}
        )
   
    task2=PythonOperator(
        task_id='name',
        python_callable=my_name
       
    )
   
    task3 = PythonOperator(
        task_id = 'my_address',
        python_callable=my_address
       
    )
   
    task1 >> task2 >> task3
