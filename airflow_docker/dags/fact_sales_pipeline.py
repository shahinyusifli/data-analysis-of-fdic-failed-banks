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
    df = df[df['Age'] <= 110]
    return df

def convert_subscription(row):
    if row['Subscription Type'] == 'Basic' and row['Monthly Revenue'] == 10:
        return 1
    elif row['Subscription Type'] == 'Basic' and row['Monthly Revenue'] == 15:
        return 2
    elif row['Subscription Type'] == 'Basic' and row['Monthly Revenue'] == 12:
        return 3
    elif row['Subscription Type'] == 'Basic' and row['Monthly Revenue'] == 13:
        return 4
    elif row['Subscription Type'] == 'Basic' and row['Monthly Revenue'] == 11:
        return 5
    elif row['Subscription Type'] == 'Basic' and row['Monthly Revenue'] == 14:
        return 6
    
    elif row['Subscription Type'] == 'Premium' and row['Monthly Revenue'] == 10:
        return 7
    elif row['Subscription Type'] == 'Premium' and row['Monthly Revenue'] == 15:
        return 8
    elif row['Subscription Type'] == 'Premium' and row['Monthly Revenue'] == 12:
        return 9
    elif row['Subscription Type'] == 'Premium' and row['Monthly Revenue'] == 13:
        return 10
    elif row['Subscription Type'] == 'Premium' and row['Monthly Revenue'] == 11:
        return 11
    elif row['Subscription Type'] == 'Premium' and row['Monthly Revenue'] == 14:
        return 12
    
    elif row['Subscription Type'] == 'Standard' and row['Monthly Revenue'] == 10:
        return 13
    elif row['Subscription Type'] == 'Standard' and row['Monthly Revenue'] == 15:
        return 14
    elif row['Subscription Type'] == 'Standard' and row['Monthly Revenue'] == 12:
        return 15
    elif row['Subscription Type'] == 'Standard' and row['Monthly Revenue'] == 13:
        return 16
    elif row['Subscription Type'] == 'Standard' and row['Monthly Revenue'] == 11:
        return 17
    elif row['Subscription Type'] == 'Standard' and row['Monthly Revenue'] == 14:
        return 18


def load_subscription_id():
    df = extract_data()
    df['subscriptionid'] = df.apply(convert_subscription, axis=1)
    print("===============================================")
    print(df['subscriptionid'].tolist())
    print("================================================")
    return df['subscriptionid'].tolist()



def load_last_payment_date():
    df = extract_data()
    df['Last Payment Date'] = pd.to_datetime(df['Last Payment Date'])
    formated_date = df['Last Payment Date'].dt.strftime('%Y-%m-%d').tolist()
    return formated_date

def load_country_id():
    df = extract_data()

    cur = conn.cursor()

    query = "select id, country from countrydimension"
    cur.execute(query)
    countries_dict = {row[1]: row[0] for row in cur.fetchall()}
    formated_country = df['Country'].map(countries_dict).tolist()

    return formated_country

def load_device_id():
    df = extract_data()

    cur = conn.cursor()

    query = "select id, device from devicedimension"
    cur.execute(query)
    devices_dict = {row[1]: row[0] for row in cur.fetchall()}
    formated_device = df['Device'].map(devices_dict).tolist()

    return formated_device


def load_data_to_fact_table():
    df = extract_data()
    df["subscriptionid"] = load_subscription_id()
    df["last_payment_date"] = load_last_payment_date()
    df['country_id'] = load_country_id()
    df['device_id'] = load_device_id()
    df.columns = df.columns.str.strip()
    df["subscriptionid"] = df["subscriptionid"].astype(int)

    cur = conn.cursor()

    for index, row in df.iterrows():
        # Check if foreign key values exist in respective dimension tables
        user_exists = check_user_exists(row['User ID'])  # You need to implement this function
        subscription_exists = check_subscription_exists(row['subscriptionid'])  # Implement this
        country_exists = check_country_exists(row['country_id'])  # Implement this
        device_exists = check_device_exists(row['device_id'])  # Implement this

        if subscription_exists and country_exists and device_exists:
            cur.execute("""INSERT INTO SalesFact (UserID, SubscriptionID, Last_Payment_Date, Country_ID, Device_ID) 
                        VALUES (%s, %s, %s, %s, %s)""",
                        (row['User ID'], row['subscriptionid'], row['last_payment_date'], row['country_id'], row['device_id'],))

    conn.commit()
    conn.close()

def check_user_exists(user_id):
    cur = conn.cursor()
    query = "SELECT COUNT(*) FROM UserDimension WHERE UserID = %s"
    cur.execute(query, (user_id,))
    count = cur.fetchone()[0]
    return count > 0

def check_subscription_exists(subscription_id):
    cur = conn.cursor()
    query = "SELECT COUNT(*) FROM SubscriptionDimension WHERE ID = %s"
    cur.execute(query, (subscription_id,))
    count = cur.fetchone()[0]
    return count > 0

def check_country_exists(country_id):
    cur = conn.cursor()
    query = "SELECT COUNT(*) FROM CountryDimension WHERE ID = %s"
    cur.execute(query, (country_id,))
    count = cur.fetchone()[0]
    return count > 0

def check_device_exists(device_id):
    cur = conn.cursor()
    query = "SELECT COUNT(*) FROM DeviceDimension WHERE ID = %s"
    cur.execute(query, (device_id,))
    count = cur.fetchone()[0]
    return count > 0


dag = DAG(
    'fact_sales_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Set the schedule interval according to your needs
)

task_extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

task_load_subscription_id = PythonOperator(
    task_id='load_subscription_id',
    python_callable=load_subscription_id,
    dag=dag,
)

task_load_last_payment_date = PythonOperator(
    task_id='load_last_payment_date',
    python_callable=load_last_payment_date,
    dag=dag,
)

task_load_country_id = PythonOperator(
    task_id='load_country_id',
    python_callable=load_country_id,
    dag=dag,
)

task_load_device_id = PythonOperator(
    task_id='load_device_id',
    python_callable=load_device_id,
    dag=dag,
)

task_load_data_to_fact_table = PythonOperator(
    task_id='load_data_to_fact_table',
    python_callable=load_data_to_fact_table,
    dag=dag,
)



# Define task dependencies
task_extract_data >> task_load_subscription_id >> task_load_last_payment_date >> task_load_country_id >> task_load_device_id >> task_load_data_to_fact_table 

