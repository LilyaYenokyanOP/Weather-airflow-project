import json
from pathlib import Path


INPUT_DIR = Path("/home/airflow/gcs/data/weather_outputs")
OUTPUT_DIR = Path("/home/airflow/gcs/data/weather_flattened")


def flatten_weather_data(input_file_path, output_dir=None):
    input_path = Path(input_file_path)
    destination_dir = Path(output_dir) if output_dir else OUTPUT_DIR
    destination_dir.mkdir(parents=True, exist_ok=True)

    with input_path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    city_name = input_path.stem.split("_from_")[0].replace("weather_in_", "")

    times = data["hourly"]["time"]
    temperatures = data["hourly"]["temperature_2m"]

    flattened_rows = []

    for time_value, temperature_value in zip(times, temperatures):
        row = {
            "city": city_name,
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "timezone": data["timezone"],
            "timestamp": time_value,
            "temperature_2m": temperature_value,
        }
        flattened_rows.append(row)

    output_file_name = f"flattened_{input_path.name}"
    output_path = destination_dir / output_file_name

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(flattened_rows, file, indent=4)

    print(f"Saved flattened data to {output_path}")

    return {
        "input_file": str(input_path),
        "output_file": str(output_path),
        "row_count": len(flattened_rows),
    }

# if __name__ == "__main__":
#     result = flatten_weather_data(
#         input_file_path="/home/lilya/Desktop/Weather-Project/airflow-weather/logs/weather_outputs/weather_in_Netherlands_from_2026-04-21_to_2026-04-21.json",
#         output_dir="/tmp/weather_flattened",
#     )
#     print(result)
