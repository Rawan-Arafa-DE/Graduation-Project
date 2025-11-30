from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from etl_ivf_pipeline import run_full_etl

default_args = {
    "owner": "elysium_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="ivf_etl_star_schema",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",   # أو None لو هتشغليه manual
    catchup=False,
    tags=["ivf", "elysium", "etl"],
) as dag:

    etl_task = PythonOperator(
        task_id="run_ivf_etl",
        python_callable=run_full_etl,
    )

    etl_task
