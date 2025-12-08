from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.append(r"/mnt/e/work/final code/graduation promax")

from etl_ivf_pipeline_final import run_full_etl

default_args = {
    "owner": "fatma",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ivf_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # تشغيل يدوي
    catchup=False,
) as dag:

    etl_task = PythonOperator(
        task_id="run_ivf_etl",
        python_callable=run_full_etl,
        op_kwargs={"refresh": False},  # faمسfalse to append and true to remove all the data
    )
