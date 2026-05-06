from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from weather_extractor import upload_to_gcs, fetch_weather_data,load_to_bigquery
from weather_transformer import flatten_weather_data

default_args = {
    "owner": "Lilya",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
# DEFAULT TODAY, AND CHOOSE DATES FOR 3 TYPES
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


def fetch_netherlands_weather(**context):
    start_date, end_date = choose_dates(**context)

    result=fetch_weather_data(
        lat=52.3676,
        lon=4.9010,
        city="Netherlands",
        start_date=start_date,
        end_date=end_date,
    )
    
    upload_to_gcs(result["file_path"])
    return result

    # return fetch_weather_data(
    #     lat=52.3676,
    #     lon=4.9010,
    #     city="Netherlands",
    #     start_date=start_date,
    #     end_date=end_date,
    # )

def load_netherlands_to_bigquery(**context):
    flatten_result = context["ti"].xcom_pull(task_ids="flatten_netherlands_weather")
    flattened_file_path = flatten_result["output_file"]
    upload_to_gcs(flattened_file_path)

    gcs_file_path=(
        f"gs://us-central1-weather-airflow-add1591d-bucket/"
        f"data/weather_outputs/{Path(flattened_file_path).name}"
    )

    load_to_bigquery(
        gcs_file_path=gcs_file_path,
        dataset_id="bronze",
        table_id="weather_raw_flattened"
    )

def fetch_yerevan_weather(**context):
    start_date, end_date=choose_dates(**context)
    result=fetch_weather_data(
        lat=40.1872,
        lon=44.5152,
        city="Yerevan",
        start_date=start_date,
        end_date=end_date,
    )
    
    upload_to_gcs(result["file_path"])
    return result
    # return fetch_weather_data(
    #     lat=40.1872,
    #     lon=44.5152,
    #     city="Yerevan",
    #     start_date=start_date,
    #     end_date=end_date,
    # )

def load_yerevan_to_bigquery(**context):
    flatten_result = context["ti"].xcom_pull(task_ids="flatten_yerevan_weather")
    flattened_file_path = flatten_result["output_file"]
    upload_to_gcs(flattened_file_path)

    gcs_file_path=(
        f"gs://us-central1-weather-airflow-add1591d-bucket/"
        f"data/weather_outputs/{Path(flattened_file_path).name}"
    )

    load_to_bigquery(
        gcs_file_path=gcs_file_path,
        dataset_id="bronze",
        table_id="weather_raw_flattened"
    )

def flatten_netherlands_weather(**context):
    fetch_result = context["ti"].xcom_pull(task_ids="fetch_netherlands_weather")
    # fetch_result = 
    return flatten_weather_data(fetch_result["file_path"])

def flatten_yerevan_weather(**context):
    fetch_result = context["ti"].xcom_pull(task_ids="fetch_yerevan_weather")
    return flatten_weather_data(fetch_result["file_path"])

# def fetch_json_from_gcs:
#      fetch_files_from_gcs
#      print(json_payload)
# #providee with raws for bgquery and destination where to keep our data in table
# def load_to_bigquery(raws, destination, **context):
#     bq_client = bigquery.Client() #making communication between our code and bigquery
#     dataset_id = "weather_dataset" 
#     table_id = "weather_data" 
#     bq_client.load_table_from_json(
#         raws, destination
        
#     )


with DAG(
    dag_id="weather_two_cities_to_bigquery",
    default_args=default_args,
    start_date=datetime(2026,4,20),
    schedule='@daily',
    catchup=False,
    params={
        "mode": Param("today", type='string', enum=['today', 'specific_day', 'date_range']),
        "target_date": Param('2026-04-22', type='string', format='date'),
        "start_date": Param('2026-04-22', type='string', format='date'),
        "end_date": Param('2026-04-22', type='string', format='date')
    }
) as dag:

    fetch_netherlands_weather_task=PythonOperator(
        task_id='fetch_netherlands_weather',
        python_callable=fetch_netherlands_weather
    )
    flatten_netherlands_task = PythonOperator(
    task_id="flatten_netherlands_weather",
    python_callable=flatten_netherlands_weather,
    )

    
    load_netherlands_to_bigquery_task=PythonOperator(
        task_id='load_netherlands_to_bigquery',
        python_callable=load_netherlands_to_bigquery
    )
    fetch_yerevan_weather_task=PythonOperator(
        task_id='fetch_yerevan_weather',
        python_callable=fetch_yerevan_weather
    )
    flatten_yerevan_task = PythonOperator(
        task_id="flatten_yerevan_weather",
        python_callable=flatten_yerevan_weather,
    )
    load_yerevan_to_bigquery_task=PythonOperator(
        task_id='load_yerevan_to_bigquery',
        python_callable=load_yerevan_to_bigquery
    )

    fetch_netherlands_weather_task >> flatten_netherlands_task >> load_netherlands_to_bigquery_task
    fetch_yerevan_weather_task >> flatten_yerevan_task >> load_yerevan_to_bigquery_task



# DEFAULT FOR TODAY
# with DAG(
#     dag_id='weather_two_cities',
#     default_args=default_args,
#     start_date=datetime(2026,4,20),
#     schedule='@daily',
#     catchup=False
# ) as dag:

#     task1=PythonOperator(
#         task_id="fetch_netherlands_weather",
#         python_callable=fetch_weather_data,
#         op_kwargs={
#             "lat": 52.3676,
#             "lon": 4.9010,
#             "city": "Netherlands",
#         },
#     )

#     task2=PythonOperator(
#         task_id='fetch_yerevan_weather',
#         python_callable=fetch_weather_data,
#         op_kwargs={
#             "lat": 40.1872,
#             "lon": 44.5152,
#             "city": "Yerevan"
#         }
#     )

#     task1 >> task2
