# load_to_db.py
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_table():
    # db connection and load logic
    # Connect to PostgreSQL
    conn = psycopg2.connect(host='host.docker.internal', 
                            dbname='airflow_svr', 
                            user='airflow', 
                            password='airflow', 
                            port='5436')

    cursor = conn.cursor()

    # Define the table
    cursor.execute("""
       -- DROP TABLE IF EXISTS data_engineering_jobs;
        CREATE TABLE IF NOT EXISTS data_engineering_jobs (
            job_id TEXT PRIMARY KEY,
            employer_website TEXT,
            job_employment_type TEXT,
            job_title TEXT,
            job_apply_link TEXT,
            job_description TEXT,
            skillset TEXT[],
            job_city TEXT,
            job_country TEXT,
            job_posted_at_date TIMESTAMP,
            employer_company_type TEXT
        );
    ALTER TABLE data_engineering_jobs ALTER COLUMN job_id TYPE text;

    """)

    # Close the connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_table()

def load_to_postgres(df):
    # db connection and load logic
    # Connect to PostgreSQL
    conn = psycopg2.connect(host='host.docker.internal', 
                            dbname='airflow_svr', 
                            user='airflow', 
                            password='airflow', 
                            port='5436')

    cursor = conn.cursor()

    # Load data
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO data_engineering_jobs (job_id, employer_website, job_employment_type, job_title, job_apply_link, job_description, skillset, job_city, job_country, job_posted_at_date, employer_company_type) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row['job_id'], row['employer_website'], row['job_employment_type'], row['job_title'], row['job_apply_link'], row['job_description'], row['skillset'], row['job_city'], row['job_country'], row['job_posted_at_date'], row['employer_company_type']))

    # Commit changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()