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


with DAG(
    dag_id="netherlands_weather_date_range",
    default_args=default_args,
    description="Fetch Netherlands weather for a fixed date range",
    schedule=None,
    start_date=datetime(2026, 4, 20),
    catchup=False,
    tags=["weather", "composer", "manual"],
) as dag:
    extract_weather_task = PythonOperator(
        task_id="fetch_weather_for_range",
        python_callable=fetch_weather_data,
        op_kwargs={
            "lat": 52.3676,
            "lon": 4.9010,
            "city": "Netherlands",
            "start_date": "2026-03-12",
            "end_date": "2026-03-31",
        },
    )

    extract_weather_task
