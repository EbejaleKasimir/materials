from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from pipeline import my_name


default_args={
    'owner': 'ebejale',
    'retries':2,
    'retry_delay':timedelta(minutes=5)
    }



def greet_them(name, age):
    print(f'my name is {name} and I am {age} years old')


#greet_them('Charles', 32)


with DAG(
    dag_id='my_python_dag_v3',
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
   
    task1 << task2

