from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

from airflow import DAG


from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

_DAG_FILE = Path(__file__).resolve()
# Jinja {% include 'sql/...' %} resolves against template_searchpath.
# SQL files live under dags/sql/, so use the dags/ folder (parent of this package).
_DAGS_ROOT = _DAG_FILE.parent.parent

sys.path.append(str(_DAG_FILE.parent))

from weather_dynamic_task_mapping import (
    fetch_weather_data,
    flatten_weather_data,
    upload_flattened_to_gcs,
    upload_to_bigquery,
)


default_args = {
    "owner": "Lilya",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def choose_dates(**context):
    params=context['params']
    mode = params['mode']
    run_date=context['ds']

    if mode=="today":
        start_date=run_date
        end_date=run_date
    elif mode == "specific_day":
        start_date=params['target_date']
        end_date=params['target_date']
    elif mode == "date_range":
        start_date=params['start_date']
        end_date=params['end_date']
    else:
        raise ValueError('mode must be today, specific_day or date_range')

    if start_date > end_date:
        raise ValueError("start_date must be earlier than or equal to end_date")
    
    return start_date, end_date

def process_city_weather(lat, lon, city, **context):
    start_date, end_date = choose_dates(**context)
    batch_id = context["run_id"]
    ingested_at = datetime.now(timezone.utc).isoformat()

    weather_data =fetch_weather_data(
        lat=lat,
        lon=lon,
        city=city,
        start_date=start_date,
        end_date=end_date,
    )

    flattened_rows=flatten_weather_data(
        weather_data=weather_data,
        city=city,
        batch_id=batch_id,
        ingested_at=ingested_at,
    )

    gcs_file_path=upload_flattened_to_gcs(
        flattened_rows=flattened_rows,
        city=city,
        start_date=start_date,
        end_date=end_date,
    )

    return gcs_file_path

def load_city_to_bigquery_mapped(**context):
    
    ti = context["ti"]
    map_index = ti.map_index
    gcs_file_path = ti.xcom_pull(
        task_ids="process_city_weather",
        key="return_value",
        map_index=map_index,
    )
    upload_to_bigquery(
        gcs_file_path=gcs_file_path,
        dataset_id="bronze",
        table_id="weather_raw_dynamic_tasks",
    )


CITIES = [
    {"lat": 52.3676, "lon": 4.9010, "city": "Amsterdam"},
    {"lat": 40.1872, "lon": 44.5152, "city": "Yerevan"},
    {"lat": 37.7749, "lon": -122.4194, "city": "San Francisco"},
    {"lat": 48.8566, "lon": 2.3522, "city": "Paris"},
    {"lat": 51.5074, "lon": -0.1278, "city": "London"},
]


with DAG(
    dag_id = "dynamic_task_mapping_cities",
    default_args= default_args,
    start_date = datetime(2026, 5, 29),
    schedule = None,
    template_searchpath=[str(_DAGS_ROOT)],
    catchup = False,
    params={
        "mode": Param("today", type='string', enum=['today', 'specific_day', 'date_range']),
        "target_date": Param('2026-05-29', type='string', format='date'),
        "start_date": Param('2026-05-29', type='string', format='date'),
        "end_date": Param('2026-05-29', type='string', format='date')
    }
) as dag:
    
    process_city_weather_task = PythonOperator.partial(
        task_id="process_city_weather",
        python_callable=process_city_weather,
    ).expand(op_kwargs=CITIES)

    load_city_to_bigquery_task = PythonOperator.partial(
        task_id="load_city_to_bigquery",
        python_callable=load_city_to_bigquery_mapped,
    ).expand(op_kwargs=[{} for _ in CITIES])

    process_city_weather_task >> load_city_to_bigquery_task

   
    bronze_to_silver_task = BigQueryInsertJobOperator(
        task_id="bronze_to_silver_weather",
        configuration={
            "query": {
                "query": "{% include 'sql/bronze_to_silver_dynamic_task.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    create_gold_tables_task = BigQueryInsertJobOperator(
        task_id="create_gold_tables",
        configuration={
            "query": {
                "query": "{% include 'sql/gold_tables_dynamic_task.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    create_mart_daily_table_task = BigQueryInsertJobOperator(
        task_id="create_mart_daily_table",
        configuration={
            "query": {
                "query": "{% include 'sql/mart_daily_table_dynamic.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    load_city_to_bigquery_task >> bronze_to_silver_task
    bronze_to_silver_task >> create_gold_tables_task >> create_mart_daily_table_task

