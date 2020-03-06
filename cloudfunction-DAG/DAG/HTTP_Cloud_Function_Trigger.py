import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.http_operator import SimpleHttpOperator
import json
from dependencies.trigger_tables import *



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG('cloud_function_trigger_4',
          default_args=default_args,
          schedule_interval=timedelta(days=1),
          )



def run_function():
    run_flow_task = SimpleHttpOperator(
        http_conn_id='cloud_function',
        method='POST',
        task_id='trigger_function',
        endpoint='run_query',
        data=json.dumps(json_data),
        headers={"Content-Type": "application/json"},
        xcom_push=True,
        dag=dag,
      )

run_function()
