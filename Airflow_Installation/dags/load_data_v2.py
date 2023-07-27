import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

# VARIABLES DECLARATION
# VARIABLES DECLARATION
API_KEY = "58ee1ddf863f2d700a6eea8c9d8ccb12"
API_ID = "a432e053"



def get_jobs(location, keyword):
    url = "https://api.adzuna.com/v1/jobs/search"
    params = {
        "location": location,
        "keyword": keyword,
        "api_key": API_KEY,
        "api_id": API_ID,
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        jobs = response.json()["results"]
        return jobs
    else:
        return None

dag = DAG(dag_id="data_engineering_jobs_dags_v2", schedule_interval="0 0 * * *")

task_get_jobs = PythonOperator(
    task_id="get_jobs",
    python_callable=get_jobs,
    op_args=["London", "data engineer"],
)

task_print_jobs = PythonOperator(
    task_id="print_jobs",
    python_callable=lambda jobs: print(jobs),
    op_args=[task_get_jobs.output],
)

task_get_jobs >> task_print_jobs
