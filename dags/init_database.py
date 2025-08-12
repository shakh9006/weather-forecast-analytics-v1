import logging
import sys

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

sys.path.append("/opt/airflow/internal")

from database.default import main
from project_config.config import DEFAULT_ARGS

DAG_ID = "init_database"
SHORT_DESC = "Init database"
TAGS = ["database", "ods", "init"]


def init_database():
    logging.info("Init database")
    main()

dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule="0 8 * * *",
    catchup=False,
    start_date=datetime(2025, 8, 12),
    description=SHORT_DESC,
    tags=TAGS,
    max_active_runs=1,
    max_active_tasks=1,
)

with dag: 
    start = EmptyOperator(task_id="start")

    database_default = PythonOperator(
        task_id="database_default",
        python_callable=main,
    )

    end = EmptyOperator(task_id="end")

    start >> database_default >> end