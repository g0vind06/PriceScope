from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator  # this will run fine inside the containers where Airflow's scheduler actually executes the job.
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    'owner': 'govind',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['tiwarigovind0601@gmail.com']
}

with DAG(
    dag_id='pricescope_pipeline',
    description='Scarpe Smartprix -> Glue bronze_to_silver -> dbt build star schema',
    default_args=default_args,
    schedule="0 6,18 * * *",  # Run at 6 AM and 6 PM UTC (8 AM and 8 PM IST)
    start_date=datetime(2026, 7, 30),
    catchup=True,
    tags=['pricescope'],
) as dag:

    scrape_smartprix = BashOperator(
        task_id='scrape_smartprix',
        bash_command=(
            'cd /opt/airflow/pricescope && python scrapers/smartprix/smartprix_scraper.py '      #possible to access smartprix scraper due to bind/volume mount of pricescope folder into Airflow container. This is the same path as in the Dockerfile.
            '--run_date "{{ dag_run.conf.get(\'run_date\', data_interval_end.strftime(\'%Y-%m-%d\')) }}" '  #.get allows for optional run_date/run_hour to be passed in via Airflow's dag_run.conf. If not provided, defaults to the data_interval_end (logical execution date) of the DAG run. data_interval_end is the end of the interval as per the schedule for which this DAG run is responsible.
            '--run_hour "{{ dag_run.conf.get(\'run_hour\', data_interval_end.strftime(\'%H\')) }}"'
        ),
        execution_timeout=timedelta(minutes=15),
    )

    bronze_to_silver = GlueJobOperator(
        task_id='bronze_to_silver',
        job_name='pricescope-bronze-to-silver',
        script_args={
            # '--run_date': '{{ ds }}',  # Pass the execution date to the Glue job. Resolves to now without having to pass it explicitly. This is the date for which we want to process the bronze data.
            # '--run_hour': '{{ data_interval_start.strftime("%H") }}',  # Pass the execution hour to the Glue job
            '--run_date': '{{ dag_run.conf.get(\'run_date\', data_interval_end.strftime(\'%Y-%m-%d\')) }}',
            '--run_hour': '{{ dag_run.conf.get(\'run_hour\', data_interval_end.strftime(\'%H\')) }}'
        },
        region_name='eu-north-1',
        wait_for_completion=True,
        execution_timeout=timedelta(minutes=15),
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            "/opt/dbt_venv/bin/dbt run "
            "--profiles-dir /opt/airflow/pricescope/dbt_project "
            "--project-dir /opt/airflow/pricescope/dbt_project "
            "--vars '{\"run_date\": \"{{ dag_run.conf.get(\'run_date\', data_interval_end.strftime(\'%Y-%m-%d\')) }}\", \"run_hour\": \"{{ dag_run.conf.get(\'run_hour\', data_interval_end.strftime(\'%H\')) }}\"}'"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=(
            "/opt/dbt_venv/bin/dbt test "
            "--profiles-dir /opt/airflow/pricescope/dbt_project "
            "--project-dir /opt/airflow/pricescope/dbt_project "
            "--vars '{\"run_date\": \"{{ dag_run.conf.get(\'run_date\', data_interval_end.strftime(\'%Y-%m-%d\')) }}\", \"run_hour\": \"{{ dag_run.conf.get(\'run_hour\', data_interval_end.strftime(\'%H\')) }}\"}'"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    scrape_smartprix >> bronze_to_silver >> dbt_run >> dbt_test