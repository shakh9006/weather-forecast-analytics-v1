import logging
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/project_config")

from config import DEFAULT_ARGS

def print_hello():
    logging.info("Start running test_dag")
    print("Run successfully!!!")
    logging.info("End running test_dag")

with DAG(
    dag_id="test_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 8, 12),
    schedule=None,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")
    test_run = PythonOperator(task_id="test_run", python_callable=print_hello)
    end = EmptyOperator(task_id="end")

    start >> test_run >> end