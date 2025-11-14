from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
import time
def hello_airflow():
    print('Hello Airflow')



def hello_user(**kwargs):
    name = kwargs['dag_run'].conf.get('message','no message')
    print(f'hello user {name}')
 
def hello(**kwargs):
    name = kwargs['dag_run'].conf.get('message','no message')
    print(f'hello user this task 3')
 

default_args = {
"retries" :1 ,
"retry_delay" : timedelta(minutes=5)
}
 
#### pipeline (DAG)
with DAG(
    dag_id = 'first_task',
    start_date = datetime(2024,12,1),
    schedule = None,
    catchup = False,
    default_args = default_args,
    params={
        'message':'enter your name'
    }
) as dag :
    t1 = PythonOperator(
        task_id='hello_user',
        python_callable=hello_user
    )

    t2 = PythonOperator(
        task_id='hello',
        python_callable=hello_airflow
    )

    t3 = PythonOperator(
        task_id='dummy',
        python_callable=hello

    )

    delay_task = PythonOperator(
        task_id="delay_python_task",
        python_callable=lambda: time.sleep(60)
    )
 
    t1  >> [delay_task , t2 ]
    delay_task >> t3
