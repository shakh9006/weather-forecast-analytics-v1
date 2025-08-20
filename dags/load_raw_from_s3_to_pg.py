import sys
import logging

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

sys.path.append("/opt/airflow/internal")

from project_config.config import DEFAULT_ARGS
from scripts.get_dates import get_dates
from scripts.storage.get_providers import get_providers
from scripts.providers.ProviderFactory import ProviderFactory

DAG_ID = "load_raw_from_s3_to_pg"
SHORT_DESC = "Load raw from S3 to PG"
TAGS = ["s3", "pg", "raw", "ods"]

def load_raw_from_s3_to_pg_handler(**context):    
    start_date, end_date = get_dates(**context)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    logging.info(f"Load raw from S3 to PG: {start_date_str} - {end_date_str}")

    providers = get_providers()
    provider_factory = ProviderFactory(providers)
    provider_factory.load_forecast_from_s3_to_pg(start_date_str, end_date_str)
    provider_factory.load_current_weather_from_s3_to_pg(start_date_str, end_date_str)


dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule="0 13 * * *",
    catchup=True,
    start_date=datetime(2025, 8, 18),
    description=SHORT_DESC,
    tags=TAGS,
    max_active_runs=1,
    max_active_tasks=1,
)

with dag:

    start = EmptyOperator(task_id="start")

    load_raw_from_s3_to_pg = PythonOperator(
        task_id="load_raw_from_s3_to_pg",
        python_callable=load_raw_from_s3_to_pg_handler,
    )

    end = EmptyOperator(task_id="end")

    start >> load_raw_from_s3_to_pg >> end