from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from function_result import call_function
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from see_logs_orm import create_logs

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 19),
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

conn = BaseHook.get_connection('postgres_ds')
conn_url = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"

dag = DAG(
    'data_function',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

log_function = PythonOperator(
    task_id='create_logs',
    python_callable=create_logs,
    op_kwargs={'param1': conn_url},
    dag=dag

)
data_function = PythonOperator(
    task_id='function_result',
    python_callable=call_function,
    op_kwargs={'param1': conn_url, 'param2': Variable.get("date")},
    dag=dag
)


send_email = EmailOperator(
    task_id='send_email',
    to='katyachelmakina@gmail.com',
    subject='ingestion complete',
    html_content="Date: {{ ds }}",
    dag=dag)

log_function >> data_function >> send_email

