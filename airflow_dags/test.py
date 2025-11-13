from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_message():
    print("Airflow DAG is working!")

with DAG(
    dag_id="test_airflow_setup",
    start_date=datetime(2025, 11, 13),
    schedule=None,       # ✅ use schedule instead of schedule_interval
    catchup=False,
) as dag:

    test_task = PythonOperator(
        task_id="print_test_message",
        python_callable=test_message
    )
