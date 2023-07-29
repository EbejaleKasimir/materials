import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

try:
    from airflow import DAG
except ImportError:
    print("The DAG class is not defined in the airflow module.")
    

default_args = {
    'owner': 'ebejale',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': ['kasimir.Ikuenobe@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_engineer_jobs_dag_v2',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

# Variables declaration
API_KEY = "58ee1ddf863f2d700a6eea8c9d8ccb12"
API_ID = "a432e053"

# Define the function to get jobs
def get_jobs(location, keyword, **context):
    url = "https://api.adzuna.com/v1/api/jobs/gb/search/1"  # updated the url as per Adzuna Job API
    params = {
        "where": location,
        "what": keyword,
        "app_id": API_ID,
        "app_key": API_KEY,
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        jobs = response.json()["results"]
        context['ti'].xcom_push(key='jobs', value=jobs)
    else:
        raise Exception(f"API request failed with status {response.status_code}")

# Define the function to print jobs
def print_jobs(**context):
    jobs = context['ti'].xcom_pull(key='jobs', task_ids='get_jobs')
    print(jobs)

# Define the DAG
dag = DAG(
    dag_id="data_engineering_jobs_dags_v2",
    default_args={
        "owner": "airflow",
        "start_date": datetime(2023, 1, 1),  # Fixed start date
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 0 * * *",
)

# Define the tasks
task_get_jobs = PythonOperator(
    task_id="get_jobs",
    python_callable=get_jobs,
    dag=dag,
    op_kwargs={"location": "London", "keyword": "data engineer"},
    provide_context=True,
)

task_print_jobs = PythonOperator(
    task_id="print_jobs",
    python_callable=print_jobs,
    dag=dag,
    provide_context=True,
)

# Set the task dependencies
task_get_jobs >> task_print_jobs
