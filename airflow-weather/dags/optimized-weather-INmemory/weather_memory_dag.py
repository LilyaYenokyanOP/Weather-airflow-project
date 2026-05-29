# task1 = fetch data in memory->flatten data in memory-> upload flatten data to gcs
# task2 = upload from gcs to bigquery
#task3= from bronze dataset upload cleaned and deduplicated data to silver dataset

# process_netherlands_task >> load_netherlands_task
# process_yerevan_task >> load_yerevan_task

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

from airflow import DAG


from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

_DAG_FILE = Path(__file__).resolve()
_DAGS_ROOT = _DAG_FILE.parent # .../dags/ — contains sql/

sys.path.append(str(_DAG_FILE.parent))

from weather_memory_extractor import (
    fetch_weather_in_memory,
    flatten_weather_in_memory,
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

    weather_data =fetch_weather_in_memory(
        lat=lat,
        lon=lon,
        city=city,
        start_date=start_date,
        end_date=end_date,
    )

    flattened_rows=flatten_weather_in_memory(
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

def load_city_to_bigquery(source_task_id, **context):
    gcs_file_path = context['ti'].xcom_pull(task_ids=source_task_id)

    upload_to_bigquery(
        gcs_file_path=gcs_file_path,
        dataset_id = "bronze",
        table_id = "weather_raw_flattened_partitioned",
    )


with DAG(
    dag_id = "weather_two_cities_bronze_silver_gold",
    default_args= default_args,
    start_date = datetime(2026, 5, 21),
    schedule=None,
    template_searchpath=[str(_DAGS_ROOT)],
    catchup=False,
    params={
        "mode": Param("today", type='string', enum=['today', 'specific_day', 'date_range']),
        "target_date": Param('2026-05-21', type='string', format='date'),
        "start_date": Param('2026-05-21', type='string', format='date'),
        "end_date": Param('2026-05-21', type='string', format='date')
    }
)as dag:
    process_netherlands_task = PythonOperator(
        task_id= "process_netherlands_weather",
        python_callable=process_city_weather,
        op_kwargs={
            "lat": 52.3676,
            "lon": 4.9010,
            "city": "Netherlands",
        }
    )

    load_netherlands_task = PythonOperator(
        task_id = "load_netherlands_to_bigquery",
        python_callable=load_city_to_bigquery,
        op_kwargs={
            "source_task_id": "process_netherlands_weather",
        }
    )

    process_yerevan_task = PythonOperator(
        task_id = "process_yerevan_weather",
        python_callable = process_city_weather,
        op_kwargs={
            "lat": 40.1872,
            "lon": 44.5152,
            "city": "Yerevan",
        }
    )

    load_yerevan_task=PythonOperator(
        task_id = "load_yerevan_to_bigquery",
        python_callable = load_city_to_bigquery,
        op_kwargs={
            "source_task_id": "process_yerevan_weather",
        }
        )
        
    bronze_to_silver_task = BigQueryInsertJobOperator(
    task_id="bronze_to_silver_weather",
    configuration={
        "query": {
            "query": "{% include 'sql/weather_bronze_to_silver.sql' %}",
            "useLegacySql": False,
        }
    },
    )

    create_gold_tables_task = BigQueryInsertJobOperator(
    task_id="create_gold_tables",
    configuration={
        "query": {
            "query": "{% include 'sql/create_gold_weather_tables.sql' %}",
            "useLegacySql": False,
        }
    },
    )

    create_mart_daily_table_task = BigQueryInsertJobOperator(
    task_id="create_mart_daily_table",
    configuration={
        "query": {
            "query": "{% include 'sql/mart_daily_table.sql' %}",
            "useLegacySql": False,
        }
    },
)
    
    
    process_netherlands_task >> load_netherlands_task >> bronze_to_silver_task
    process_yerevan_task >> load_yerevan_task >> bronze_to_silver_task

    bronze_to_silver_task >> create_gold_tables_task >> create_mart_daily_table_task

