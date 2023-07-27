from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import load_to_db
import airflow

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
    'data_engineer_jobs_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

# VARIABLES DECLARATION
url = "https://jsearch.p.rapidapi.com/search"
querystring = {"query":"Data Engineer in Ontario, Canada","page":"1","num_pages":"1","date_posted":"month"}
headers = {
    "X-RapidAPI-Key": "a0139f52d4mshf129fb600694e41p10962ejsnd689f7c88b89",
    "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
}

# LIST OF SKILLS
words = ['ETL','Orchestration', 'modeling', 'python', 
         'sql','pandas','docker','aws','gcp','google cloud',
         'postgres','mongodb','spark','jira','databricks',
         'azure','dbt','amazon','s3','linux','hadoop','kubernetes',
         'hbase','hive','fivetran','mage','airflow','ci/cd','elt']

acronyms = ['sql','dbt','elt','etl','aws','gcp'] # Acronyms from skills list that would need to be made uppercase

# Create a function to extract skills from an input
def extract_skills(c):
    skills = []
    for i in words:
        if i.lower() in c.lower():
            if i.lower() in acronyms:
                skills.append(i.upper())
            else: 
                skills.append(i.title())
    return skills

def get_data_from_api():
    response = requests.get(url, headers=headers, params=querystring)
    data = response.json()
    return data

def extract_relevant_records_from_overall_data(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='get_data_from_api')
    # Initialize lists to store data
    employer_website = []
    job_id = []
    job_employment_type = []
    job_title = []
    job_apply_link=[]
    job_description=[]
    job_city=[]
    job_country =[]
    job_posted_at_date =[]
    employer_company_type =[]

    for i in range(len(data['data'])):
        employer_website.append(data['data'][i]['employer_website'])
        job_id.append(data['data'][i]['job_id'])
        job_employment_type.append(data['data'][i]['job_employment_type'])
        job_title.append(data['data'][i]['job_title'])
        job_apply_link.append(data['data'][i]['job_apply_link'])
        job_description.append(data['data'][i]['job_description'])
        job_city.append(data['data'][i]['job_city'])
        job_country.append(data['data'][i]['job_country'])
        job_posted_at_date.append(data['data'][i]['job_posted_at_datetime_utc'][:10])
        employer_company_type.append(data['data'][i]['employer_company_type'])

    return employer_website, job_id, job_employment_type, job_title, job_apply_link, job_description, job_city, job_country, job_posted_at_date, employer_company_type
    pass

def translate_extractions_to_dataframe_and_transform(**context):
    ti = context['ti']
    records = ti.xcom_pull(task_ids='extract_relevant_records_from_overall_data')
    employer_website, job_id, job_employment_type, job_title, job_apply_link, job_description, job_city, job_country, job_posted_at_date, employer_company_type = records

    # Placing values into columns
    rapid_dict = {
                    'job_id': job_id,
                    'employer_website':employer_website,
                    'job_employment_type':job_employment_type,
                    'job_title':job_title,
                    'job_apply_link':job_apply_link,
                    'job_description':job_description,
                    'job_city':job_city,
                    'job_country':job_country,
                    'job_posted_at_date':job_posted_at_date,
                    'employer_company_type':employer_company_type        
                 }
    job_df = pd.DataFrame(rapid_dict) # Convert to dataframe
    
    # Convert date column datatype from string to datetime
    job_df['job_posted_at_date'] = pd.to_datetime(job_df['job_posted_at_date'])
    
    # Add a new column in the dataframe and extract skills from the job_description column
    # Using the job_description column as an input in the extract_skills function
    job_df['skillset'] = job_df['job_description'].apply(lambda x: extract_skills(x))
    
    # Change the position of the skillset column from last to after the job_description column
    # Remove the skillset column and save it in a variable
    skillset_col = job_df.pop('skillset')
    
    # Insert column using insert(position,column_name,skillset_col) function
    job_df.insert(6, 'skillset', skillset_col)
  
    return job_df

def create_table():
    load_to_db.create_table()

def load_to_postgres(**context):
    ti = context['ti']
    df = ti.xcom_pull(task_ids='translate_extractions_to_dataframe_and_transform')
    load_to_db.load_to_postgres(df)

t1 = PythonOperator(
    task_id='get_data_from_api',
    python_callable=get_data_from_api,
    dag=dag,
)

t2 = PythonOperator(
    task_id='extract_relevant_records_from_overall_data',
    python_callable=extract_relevant_records_from_overall_data,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='translate_extractions_to_dataframe_and_transform',
    python_callable=translate_extractions_to_dataframe_and_transform,
    provide_context=True,
    dag=dag,
)

t4 = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)

t5 = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag,
)

t1 >> t2 >> t3 >> t4 >> t5
