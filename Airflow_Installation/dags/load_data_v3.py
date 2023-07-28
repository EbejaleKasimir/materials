import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


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


# Define the function to write to CSV
def write_to_csv(**context):
    jobs = context['ti'].xcom_pull(key='jobs', task_ids='get_jobs')
    df = pd.DataFrame(jobs)
    df.to_csv('jobs.csv', index=False)

# Define the function to write to PostgreSQL
def write_to_postgres(**context):
    jobs = context['ti'].xcom_pull(key='jobs', task_ids='get_jobs')
    df = pd.DataFrame(jobs)

    # Define our connection string
    conn_string = "host='host.docker.internal' dbname='airflow' user='postgres' password='airflow'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    # Prepare the INSERT statement
    insert_statement = 'INSERT INTO jobs ({}) VALUES %s'.format(','.join(df.columns))

    # Execute the statement
    execute_values(cursor, insert_statement, list(df.itertuples(index=False, name=None)))
    conn.commit()

    # Close the connection
    cursor.close()
    conn.close()


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

task_write_to_csv = PythonOperator(
    task_id="write_to_csv",
    python_callable=write_to_csv,
    dag=dag,
    provide_context=True,
)

task_write_to_postgres = PythonOperator(
    task_id="write_to_postgres",
    python_callable=write_to_postgres,
    dag=dag,
    provide_context=True,
)

# Set the task dependencies
(task_get_jobs >> task_write_to_csv >> task_print_jobs)
(task_get_jobs >> task_write_to_postgres >> task_print_jobs)
