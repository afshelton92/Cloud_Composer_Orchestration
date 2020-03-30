import airflow
from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import models
from ncaa_dependencies.table_schemas import *
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #'start_date': airflow.utils.dates.days_ago(2),
    'start_date': datetime(2019, 6, 11, 14, 30),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG('NCAA_View_to_Table',
          default_args=default_args,
          schedule_interval=timedelta(days=1),
          catchup=False
          )

start = BashOperator(task_id='start_message',
                     bash_command='echo "started"',
                     dag=dag)


end = BashOperator(task_id='end_message',
                   bash_command='echo "Finished"',
                   dag=dag)



for table in TABLES:

    query = BigQueryOperator(
    sql=f'SELECT * FROM {table}',
    task_id=f'append_{table}_to_table',
    destination_dataset_table=f'{APPEND[table]}',
    allow_large_results=True,
    bigquery_conn_id='bigquery_default',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_APPEND',
    use_legacy_sql=False,
    dag=dag
    )
    start >> query >> end
