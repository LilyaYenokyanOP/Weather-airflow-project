import argparse
import json
import re
from datetime import date
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
from google.cloud import storage

# local file is saved in external_storage and the upload target in cloud bucket folder weather_outputs, so we have a clear separation between local and cloud storage. This also allows us to keep a local copy of the fetched data for debugging or backup purposes, while ensuring that the data is also available in the cloud for further processing and analysis.
DEFAULT_OUTPUT_DIR = Path("/home/airflow/gcs/external_storage/weather_outputs")
# in my current code external_storage is a loacl filesystem folder path
# LOCAL_FALLBACK_OUTPUT_DIR = Path("/opt/airflow/logs/weather_outputs")
# LOCAL_BACKUP_DIR = Path('/home/airflow/gcs/external_storage/weather_outputs') #local backup directory, for the local save path
# GCS_UPLOAD_PREFIX = "data/weather_outputs/"  # prefix in GCS bucket where files will be uploaded

# function below cleans city names to create safe file names, replacing spaces and special characters with underscores, and ensuring no leading or trailing underscores remain. If the cleaned name is empty, it defaults to "unknown_location".
def _safe_file_fragment(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", value.strip())
    return cleaned.strip("_") or "unknown_location"


# connection to gcs
# just fetched data from local storage and upload to gcs
# result is that fetched weather file is now in the gcs bucket
def upload_to_gcs(local_file_path):
    storage_client = storage.Client()  # create gcs client
    bucket = storage_client.bucket('us-central1-weather-airflow-add1591d-bucket')  # get bucket where we keep our data
    # Extract filename from local path for GCS blob name
    filename = Path(local_file_path).name
    blob = bucket.blob(f"data/weather_outputs/{filename}")  # upload to weather_outputs/ folder in GCS
    blob.upload_from_filename(local_file_path)  # upload local file to GCS
    print(f"Uploaded {filename} to GCS bucket {bucket.name} at data/weather_outputs/{filename}")  # Add success log


def fetch_weather_data(
    lat,
    lon,
    start_date=None,
    end_date=None,
    city="",
    output_dir=None,
):
    url = "https://api.open-meteo.com/v1/forecast"

    if not start_date:
        start_date = date.today().isoformat()
    if not end_date:
        end_date = start_date

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m",
        "timezone": "auto",
    }

    if output_dir:
        destination_dir = Path(output_dir)
    else:
        destination_dir = DEFAULT_OUTPUT_DIR


    print(f"Fetching weather for {city or 'selected location'}: {start_date} to {end_date}")
    request_url = f"{url}?{urlencode(params)}"

    try:
        with urlopen(request_url, timeout=30) as response:
            result = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"Weather API returned HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"Weather API request failed: {exc.reason}") from exc

    safe_city = _safe_file_fragment(city)
    file_name = f"weather_in_{safe_city}_from_{start_date}_to_{end_date}.json"
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination_dir = output_dir or LOCAL_BACKUP_DIR
    file_path = destination_dir / file_name

    with file_path.open("w", encoding="utf-8") as file:
        json.dump(result, file, indent=4)

    print(f"Saved weather data to {file_path}")
    return {
        "file_path": str(file_path),
        "city": city,
        "start_date": start_date,
        "end_date": end_date,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Professional Weather Data Extractor")
    
    parser.add_argument("--lat", type=float, default=52.3676, help="Latitude")
    parser.add_argument("--lon", type=float, default=4.901, help="Longitude")
    parser.add_argument("--city", default="Netherlands", help="City label for output file")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--output-dir", help="Directory for saved JSON files")
    
    args = parser.parse_args()

    
    result=fetch_weather_data(
        lat=args.lat,
        lon=args.lon,
        city=args.city,
        start_date=args.start,
        end_date=args.end,
        output_dir=args.output_dir,
    )
    
    upload_to_gcs(result["file_path"])

    
   
