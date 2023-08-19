from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 16),
    'retries': 1
}

csv_filepath = '/opt/airflow/dags/Netflix_dataset.csv'
df = pd.read_csv(csv_filepath, sep=";")
conn = psycopg2.connect(
    dbname='netflix_dw',
    user='postgres',
    password='inbanktask',
    host='host.docker.internal',
    port='5432'
)

def import_unique_genders_to_dimension():

    unique_genders = df['Gender'].unique()  # Extract unique gender values
    cur = conn.cursor()

    for gender in unique_genders:
        cur.execute("INSERT INTO GenderDimension (Gender) VALUES (%s)", (gender,))

    conn.commit()
    conn.close()

def import_unique_countires_to_dimension():
     
    unique_countries = df['Country'].unique()
    cur = conn.cursor()

    for country in unique_countries:
        cur.execute("INSERT INTO CountryDimension (Country) VALUES (%s)", (country,))

    conn.commit()
    conn.close()

def import_unique_devices_to_dimension():
     
    unique_devices = df['Device'].unique()
    cur = conn.cursor()

    for device in unique_devices:
        cur.execute("INSERT INTO DeviceDimension (Device) VALUES (%s)", (device,))

    conn.commit()
    conn.close()


def import_unique_subscription_to_dimension():
     
    unique_devices = df['Subscription Type'].unique()
    
    cur = conn.cursor()

    for device in unique_devices:
        cur.execute("INSERT INTO DeviceDimension (Device) VALUES (%s)", (device,))

    conn.commit()
    conn.close()


dag = DAG(
    'import_unique_genders_to_dimension',
    default_args=default_args,
    schedule_interval=None,  # Set the schedule interval according to your needs
)

task_import_unique_genders_to_dimension = PythonOperator(
    task_id='import_unique_genders_to_dimension',
    python_callable=import_unique_genders_to_dimension,
    dag=dag,
)

task_import_unique_countries_to_dimension = PythonOperator(
    task_id='import_unique_countries_to_dimension',
    python_callable=import_unique_countires_to_dimension,
    dag=dag,
)

task_import_unique_devices_to_dimension = PythonOperator(
    task_id='import_unique_devices_to_dimension',
    python_callable=import_unique_devices_to_dimension,
    dag=dag,
)



# Define task dependencies
task_import_unique_genders_to_dimension >> task_import_unique_countries_to_dimension
