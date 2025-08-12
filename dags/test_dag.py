from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

def print_hello():
    print("Run successfully!!!")

with DAG(
    dag_id="test_dag",
    start_date=datetime(2025, 8, 12),
    schedule=None,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")
    test_run = PythonOperator(task_id="test_run", python_callable=print_hello)
    end = EmptyOperator(task_id="end")

    start >> test_run >> end