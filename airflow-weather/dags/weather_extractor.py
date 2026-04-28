import argparse
import json
import re
from datetime import date
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


DEFAULT_OUTPUT_DIR = Path("/home/airflow/gcs/data/weather_outputs")
# LOCAL_FALLBACK_OUTPUT_DIR = Path("/opt/airflow/logs/weather_outputs")



def _safe_file_fragment(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", value.strip())
    return cleaned.strip("_") or "unknown_location"


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

    
    fetch_weather_data(
        lat=args.lat,
        lon=args.lon,
        city=args.city,
        start_date=args.start,
        end_date=args.end,
        output_dir=args.output_dir,
    )
   
