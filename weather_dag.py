from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from weather_extractor import fetch_weather_data


default_args = {
    "owner": "Lilya",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_daily_weather_extract(**context):
    run_date = context["ds"]
    return fetch_weather_data(
        lat=52.3676,
        lon=4.9010,
        city="Netherlands",
        start_date=run_date,
        end_date=run_date,
    )


with DAG(
    dag_id="netherlands_weather_pipeline",
    default_args=default_args,
    description="Fetch Netherlands weather daily and store the JSON in Composer data storage",
    schedule="@daily",
    start_date=datetime(2026, 4, 20),
    catchup=False,
    tags=["weather", "composer"],
) as dag:
    extract_weather_task = PythonOperator(
        task_id="fetch_weather_from_api",
        python_callable=run_daily_weather_extract,
    )

    extract_weather_task
