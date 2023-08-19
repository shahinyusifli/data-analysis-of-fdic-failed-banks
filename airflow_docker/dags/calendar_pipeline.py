from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2

# Define your PostgreSQL connection parameters
postgres_conn = {
    'dbname': 'netflix_dw',
    'user': 'postgres',
    'password': 'inbanktask',
    'host': 'host.docker.internal',
    'port': '5432'
}

# Define the default arguments for the DAG
default_args = {
    'owner': 'shahin',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'populate_time_dimension',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False  # Do not backfill for past dates
)

def populate_time_dimension():
    conn = psycopg2.connect(**postgres_conn)
    cur = conn.cursor()
    
    # Your PostgreSQL query here
    query = """-- Generate a series of dates from 2021-01-01 to 2024-12-31
INSERT INTO datedimension (ID, Day, Month, MonthName, Year, Quarter, Weekday, DayOfWeekName, IsWeekend)
SELECT 
    d::DATE AS ID,
    EXTRACT(DAY FROM d) AS Day,
    EXTRACT(MONTH FROM d) AS Month,
    TO_CHAR(d, 'Month') AS MonthName,
    EXTRACT(YEAR FROM d) AS Year,
    EXTRACT(QUARTER FROM d) AS Quarter,
    EXTRACT(ISODOW FROM d) AS Weekday,
    TO_CHAR(d, 'Day') AS DayOfWeekName,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS IsWeekend
FROM generate_series(
    '2021-01-01'::DATE,
    '2024-12-31'::DATE,
    interval '1 day') AS d;
    """
    
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

# Define the PythonOperator to execute the task
populate_time_dimension_task = PythonOperator(
    task_id='populate_time_dimension',
    python_callable=populate_time_dimension,
    dag=dag
)

# Set task dependencies
populate_time_dimension_task

# If there are other tasks to run after populating the TimeDimension table, add them here
# task_after_populating = ...

# populate_time_dimension_task >> task_after_populating
