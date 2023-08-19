from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import json

default_args = {
    'owner': 'shahin',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 16),
    'retries': 1
}

def extract_data():
    csv_filepath = '/opt/airflow/dags/Netflix_dataset.csv'
    df = pd.read_csv(csv_filepath, sep=";")
   
    return df

def check_quality_with_list():
    df = extract_data()
    extracted_countries = df['Country'].unique()
    with open('/opt/airflow/dags/country_pipeline/countries.json', 'r') as file:
        countries_data = json.load(file)
    countries_list = countries_data['countries']

    for s in extracted_countries:
        if s not in countries_list:
            print(f'{s} is invalid for country name')

def load_countries_to_dim():
    df = extract_data()
    conn = psycopg2.connect(
        dbname='netflix_dw',
        user='postgres',
        password='inbanktask',
        host='host.docker.internal',
        port='5432'
    )
    unique_countries = df['Country'].unique()
    cur = conn.cursor()

    for country in unique_countries:
        cur.execute("SELECT * FROM CountryDimension WHERE country = %s", (country,))
        existing_data = cur.fetchone()

        if existing_data is None:
            cur.execute("INSERT INTO CountryDimension (Country) VALUES (%s)", (country,))
    
    conn.commit()
    conn.close()


dag = DAG(
    'country_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Set the schedule interval according to your needs
)

task_extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

task_load_countries_to_dim = PythonOperator(
    task_id='load_countries_to_dim',
    python_callable=load_countries_to_dim,
    dag=dag,
)

task_check_quality_with_list = PythonOperator(
    task_id='check_quality_with_list',
    python_callable=check_quality_with_list,
    dag=dag,
)



# Define task dependencies
task_extract_data >> task_check_quality_with_list >> task_load_countries_to_dim
