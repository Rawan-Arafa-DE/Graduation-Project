from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='etl_project_fatma',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etl'],
) as dag:

    extract_task = BashOperator(
        task_id='extract_data',
        bash_command='python /opt/airflow/dags/scripts/extract.py'
    )

    transform_task = BashOperator(
        task_id='transform_data',
        bash_command='python /opt/airflow/dags/scripts/transform.py'
    )

    load_task = BashOperator(
        task_id='load_data',
        bash_command='python /opt/airflow/dags/scripts/load.py'
    )

    extract_task >> transform_task >> load_task
