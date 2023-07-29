import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Variables declaration
API_KEY = "58ee1ddf863f2d700a6eea8c9d8ccb12"
API_ID = "a432e053"
TABLE_COLUMNS = [
    "id", "salary_min", "salary_max", "description", "category", "label", "tag", "location", 
    "longitude", "latitude", "title", "salary_is_predicted", "adref", "created", "redirect_url", 
    "company", "class", "area"
]

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
        filtered_jobs = [{col: job.get(col, None) for col in TABLE_COLUMNS} for job in jobs]
        context['ti'].xcom_push(key='jobs', value=filtered_jobs)
    else:
        raise Exception(f"API request failed with status {response.status_code}")

# Define the function to write to CSV
def write_to_csv(**context):
    jobs = context['ti'].xcom_pull(key='jobs', task_ids='get_jobs')
    df = pd.DataFrame(jobs)
    df.to_csv('jobs.csv', index=False)
	
def write_to_postgres(**context):
    hook = PostgresHook(postgres_conn_id='my_connection')
    jobs = context['ti'].xcom_pull(key='jobs', task_ids='get_jobs')
    df = pd.DataFrame(jobs)

    # Convert dictionary columns into strings
    for col in df.columns:
        if isinstance(df[col].iloc[0], dict):
            df[col] = df[col].apply(str)

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                DROP TABLE IF EXISTS jobs;
                CREATE TABLE jobs(
                    id VARCHAR(255),
                    salary_min DECIMAL,
                    salary_max DECIMAL,
                    description TEXT,
                    category TEXT,
                    label TEXT,
                    tag TEXT,
                    location TEXT,
                    longitude DECIMAL,
                    latitude DECIMAL,
                    title TEXT,
                    salary_is_predicted BOOLEAN,
                    adref TEXT,
                    created TIMESTAMP,
                    redirect_url TEXT,
                    company TEXT,
                    class TEXT,
                    area TEXT
                )
            """)

            # Insert records
            execute_values(
                cursor,
                f"INSERT INTO jobs ({','.join(df.columns)}) VALUES %s",
                [tuple(x) for x in df.values]
            )

        conn.commit()

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('job_data_pipeline', default_args=default_args, description='A simple job data pipeline', schedule_interval=timedelta(days=1))

get_jobs_task = PythonOperator(
    task_id='get_jobs',
    python_callable=get_jobs,
    op_kwargs={'location': 'London', 'keyword': 'engineer'},
    provide_context=True,
    dag=dag,
)

write_to_csv_task = PythonOperator(
    task_id='write_to_csv',
    python_callable=write_to_csv,
    provide_context=True,
    dag=dag,
)

write_to_postgres_task = PythonOperator(
    task_id='write_to_postgres',
    python_callable=write_to_postgres,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
get_jobs_task >> write_to_csv_task >> write_to_postgres_task
