from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from spotify_etl import run_etl_spotify

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 8, 25),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='¡Nuestro primer DAG con proceso ETL!',
    schedule_interval=timedelta(days=1),
)


def just_a_function():
    print("Hola")


run_etl = PythonOperator(
    task_id='spotify',
    python_callable=run_etl_spotify,
    dag=dag,
)

