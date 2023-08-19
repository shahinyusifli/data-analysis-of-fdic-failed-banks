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

conn = psycopg2.connect(
        dbname='netflix_dw',
        user='postgres',
        password='inbanktask',
        host='host.docker.internal',
        port='5432'
    )

def extract_data():
    csv_filepath = '/opt/airflow/dags/Netflix_dataset.csv'
    df = pd.read_csv(csv_filepath, sep=";")
   
    return df

def delete_outlier_age():
    df = extract_data()
    outlier_age_free_df = df[df['Age'] <= 110]
    return outlier_age_free_df


def format_join_date():
    df = delete_outlier_age()
    df['Join Date'] = pd.to_datetime(df['Join Date'])
    formated_date = df['Join Date'].dt.strftime('%Y-%m-%d').tolist()
    return formated_date

def format_genders():
    df = delete_outlier_age()

    cur = conn.cursor()

    query = "select id, gender from genderdimension"
    cur.execute(query)
    genders_dict = {row[1]: row[0] for row in cur.fetchall()}
    formated_gender = df['Gender'].map(genders_dict).tolist()

    return formated_gender

def load_users_to_dim():
    df = delete_outlier_age()
    df["Join Date"] = format_join_date()
    df["Gender_ID"] = format_genders()
    df.columns = df.columns.str.strip()


    cur = conn.cursor()

    for index, row in df.iterrows():
        cur.execute("""INSERT INTO UserDimension (UserID, JoinDate, Age, ActiveProfiles, HouseholdProfileInd, MoviesWatched, SeriesWatched, Gender_ID) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", 
                    (row['User ID'], row['Join Date'], row['Age'], row['Active Profiles'], row['Household Profile Ind'], row['Movies Watched'], row['Series Watched'], row['Gender_ID']))
    
    
    
    conn.commit()
    conn.close()



dag = DAG(
    'user_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Set the schedule interval according to your needs
)

task_extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

task_delete_outlier_age = PythonOperator(
    task_id='delete_outlier_age',
    python_callable=delete_outlier_age,
    dag=dag,
)

task_format_join_date = PythonOperator(
    task_id='format_join_date',
    python_callable=format_join_date,
    dag=dag,
)

task_format_genders = PythonOperator(
    task_id='format_genders',
    python_callable=format_genders,
    dag=dag,
)

task_load_users_to_dim = PythonOperator(
    task_id='load_users_to_dim',
    python_callable=load_users_to_dim,
    dag=dag,
)



# Define task dependencies
task_extract_data >> task_delete_outlier_age >> task_format_join_date >> task_format_genders >> task_load_users_to_dim
