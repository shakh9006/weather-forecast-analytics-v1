import sys
import logging

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

sys.path.append("/opt/airflow/internal")

from project_config.config import DEFAULT_ARGS
from scripts.get_dates import get_dates
from scripts.storage.get_countries import get_countries
from scripts.storage.get_providers import get_providers
from scripts.providers.ProviderFactory import ProviderFactory

DAG_ID = "extract_raw_from_api_to_s3"
SHORT_DESC = "Extract raw from API to S3"
TAGS = ["api", "s3", "raw", "ods"]

def extract_raw_from_api_to_s3_handler(**context):    
    start_date, end_date = get_dates(**context)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    countries = get_countries()
    providers = get_providers()
    provider_factory = ProviderFactory(providers, countries)
    provider_factory.run_all_providers(start_date_str, end_date_str)

    logging.info(f"Extract raw from API to S3: {start_date_str} - {end_date_str}")

dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule="0 12 * * *",
    catchup=False,
    start_date=datetime(2025, 8, 12),
    description=SHORT_DESC,
    tags=TAGS,
    max_active_runs=1,
    max_active_tasks=1,
)

with dag:

    start = EmptyOperator(task_id="start")

    extract_raw_from_api_to_s3 = PythonOperator(
        task_id="extract_raw_from_api_to_s3",
        python_callable=extract_raw_from_api_to_s3_handler,
    )

    end = EmptyOperator(task_id="end")

    start >> extract_raw_from_api_to_s3 >> end