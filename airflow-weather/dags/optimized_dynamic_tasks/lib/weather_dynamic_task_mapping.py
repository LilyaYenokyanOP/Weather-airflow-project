# functions
# fetch_weather_in_memory()
# flatten_weather_in_memory()
# upload_flattened_to_gcs()
# load_to_bigquery()

import json
import re
from datetime import date
from urllib.parse import urlencode # for constructing the API request URL with query parameters
from urllib.request import urlopen
from google.cloud import storage, bigquery


# BUCKET_NAME = "us-central1-weather-airflow-add1591d-bucket"
BUCKET_NAME = "us-central1-weather-airflow-4c2c14a3-bucket"


def _safe_file_fragment(value):
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", value.strip())
    return cleaned.strip("_") or "unknown_location"

# 1.fetch API-> return Python dict with weather data
def fetch_weather_data(lat, lon, city, start_date=None, end_date=None):
    url = "https://api.open-meteo.com/v1/forecast"

    if not start_date:
        start_date=date.today().isoformat()
    if not end_date:
        end_date=start_date
    
    params={
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,     
        "end_date": end_date,
        "hourly": "temperature_2m",
        "timezone": "auto",
    }
    print(f"Fetching weather for {city or 'selected location'}: {start_date} to {end_date}")

    request_url =f"{url}?{urlencode(params)}"

    with urlopen(request_url, timeout=30) as response:
        weather_data = json.loads(response.read().decode("utf-8")) # creates Python dict in memory with the weather data

    return weather_data


# 2.flattening data from fetched data in memory
def flatten_weather_data(weather_data, city, batch_id, ingested_at):
    rows=[]
    times=weather_data["hourly"]["time"]
    temperatures = weather_data["hourly"]["temperature_2m"]

    for time_value, temperature_value in zip(times, temperatures):
        row={
            "city": city,
            "latitude": weather_data["latitude"],
            "longitude": weather_data["longitude"],
            "timezone": weather_data["timezone"],
            "timestamp": time_value,
            "temperature_2m": temperature_value,
            "batch_id": batch_id,
            "ingested_at": ingested_at,
        }

        rows.append(row)
    
    return rows


# # 3.upload flattened data to gcs
def upload_flattened_to_gcs(flattened_rows, city, start_date, end_date):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    safe_city = _safe_file_fragment(city)
    file_name = f"flattened_weather_in_{safe_city}_from_{start_date}_to_{end_date}.json"
    blob_path = f"data/weather_flattened_inMemory/{file_name}" 
    blob= bucket.blob(blob_path)   

    ndjson_text = ""
    for row in flattened_rows:
        ndjson_text+= json.dumps(row) + "\n"
    
    blob.upload_from_string(ndjson_text, content_type="application/json")

    gcs_file_path = f"gs://{bucket.name}/{blob_path}"
    print(f"Uploaded {len(flattened_rows)} flattened rows to GCS at {gcs_file_path}")
    return gcs_file_path


# 4.upload to BigQuery
def upload_to_bigquery(gcs_file_path, dataset_id, table_id):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)

    schema=[
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
            bigquery.SchemaField("timezone", "STRING"),
            bigquery.SchemaField("timestamp", "STRING"),
            bigquery.SchemaField("temperature_2m", "FLOAT"),
            bigquery.SchemaField("batch_id", "STRING"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        autodetect=False,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ingested_at",
        ),
        clustering_fields=["city", "batch_id"],
    )

    load_job = client.load_table_from_uri(
        gcs_file_path,
        table_ref,
        job_config=job_config
    )

    load_job.result()
    print(f"Loaded {load_job.output_rows} rows from {gcs_file_path} to BigQuery table {dataset_id}.{table_id}")

# testing fetched data
# if __name__ == "__main__":
#     result = fetch_weather_in_memory(
#         lat=52.3676,
#         lon=4.9010,
#         city="Netherlands",
#         start_date="2026-05-01",
#         end_date="2026-05-05",
#     )

#     print(type(result))
#     print(result.keys())
#     print(json.dumps(result, indent=4)[:1000])
#     print(result["hourly"].keys())
#     print(result["hourly"]["time"][:3])
#     print(result["hourly"]["temperature_2m"][:3])

# testing flattening data
# if __name__ == "__main__":
#     result = fetch_weather_in_memory(
#         lat=52.3676,
#         lon=4.9010,
#         city="Netherlands",
#         start_date="2026-05-01",
#         end_date="2026-05-05",
#     )

#     flattened_rows = flatten_weather_in_memory(result, city="Netherlands")

#     print(f"Flattened rows: {len(flattened_rows)}")
#     print(json.dumps(flattened_rows[:48], indent=4))
