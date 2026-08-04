from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator  

with DAG(
    dag_id='failure_alert_test_dag',
    schedule_interval=None,
    catchup=False,
) as dag:

    test_alert = BashOperator(
        task_id='test_alert',
        bash_command='exit 1',  # This command will fail, triggering the on_failure_callback
        email_on_failure=True,
        email=['tiwarigovind0601@gmail.com'],
    )

