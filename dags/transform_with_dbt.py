import sys
import logging

from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

sys.path.append("/opt/airflow/internal")

from project_config.config import DEFAULT_ARGS

DAG_ID = "transform_with_dbt"
SHORT_DESC = "Transform with dbt"
TAGS = ["dbt", "transform", "ods", "mart"]   

dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule="0 14 * * *",
    catchup=False,
    start_date=datetime(2025, 8, 12),
    description=SHORT_DESC,
    tags=TAGS,
    max_active_runs=1,
    max_active_tasks=1,
)
with dag:


    start = EmptyOperator(task_id="start")

    transform_with_dbt = DockerOperator(
        task_id='dbt-orchestrator',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command='run',
        working_dir='/usr/app',
        network_mode='weather-forecast-analytics-v1_default',
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source='/Users/swift/Documents/learning/de/projects/weather-forecast-analytics-v1/dbt/weather_forecast_analytics',
                target='/usr/app',
                type='bind',
            ),
            Mount(
                source='/Users/swift/Documents/learning/de/projects/weather-forecast-analytics-v1/dbt/profiles.yml',
                target='/root/.dbt/profiles.yml',
                type='bind',
            ),
        ],
        docker_url='unix://var/run/docker.sock',
        auto_remove='success',
    )

    end = EmptyOperator(task_id="end")

    start >> transform_with_dbt >> end