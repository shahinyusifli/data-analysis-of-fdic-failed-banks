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
    extracted_devices = df['Device'].unique()
    with open('/opt/airflow/dags/device_pipeline/devices.json', 'r') as file:
        devices_data = json.load(file)
    devices_list = devices_data['devices']

    for s in extracted_devices:
        if s not in devices_list:
            print(f'{s} is invalid for device name')

def load_devices_to_dim():
    df = extract_data()
    conn = psycopg2.connect(
        dbname='netflix_dw',
        user='postgres',
        password='inbanktask',
        host='host.docker.internal',
        port='5432'
    )
    unique_devices = df['Device'].unique()
    cur = conn.cursor()

    for device in unique_devices:
        cur.execute("SELECT * FROM DeviceDimension WHERE device = %s", (device,))
        existing_data = cur.fetchone()

        if existing_data is None:
            cur.execute("INSERT INTO DeviceDimension (device) VALUES (%s)", (device,))
    
    conn.commit()
    conn.close()


dag = DAG(
    'device_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Set the schedule interval according to your needs
)

task_extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

task_load_devices_to_dim = PythonOperator(
    task_id='load_devices_to_dim',
    python_callable=load_devices_to_dim,
    dag=dag,
)

task_check_quality_with_list = PythonOperator(
    task_id='check_quality_with_list',
    python_callable=check_quality_with_list,
    dag=dag,
)



# Define task dependencies
task_extract_data >> task_check_quality_with_list >> task_load_devices_to_dim
