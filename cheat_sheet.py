# import requests
# import json
# import argparse
# from datetime import date

# # TODAYS WEATHER
# # def todays_weather(lat, lon, city_name):



# #     url = "https://api.open-meteo.com/v1/forecast"
# #     today = date.today().isoformat()

# #     params ={
# #         "latitude": lat,
# #         "longitude": lon,
# #         "start_date": today,
# #         "end_date": today,
# #         "hourly": "temperature_2m",
# #         "timezone": "auto"
# #     }

# # # extraction
# #     print(f"Fetching weather for {city_name} ({today})...")
# #     response = requests.get(url,params=params)

# # # loading
# #     if response.status_code == 200:
# #         result=response.json()
# #         file_name=f"today_weather_{city_name}_{today}.json"

# #         with open(file_name, "w") as file:
# #             json.dump(result,  file, indent=4)
# #         print(f"✅ Success! Data saved to {file_name}")
# #         return result
# #     else:
# #         print(f"❌ Error: Could not fetch data. Status: {response.status_code}")
# #         return None 

# # yerevan_todays_weather=todays_weather(40.18, 44.51, "Yerevan")


# # SPECIFIC DATE WEATHER
# # def spec_date_weather(target_date):
# #     url = "https://api.open-meteo.com/v1/forecast"
    
# #     params ={
# #         "latitude": 40.18,
# #         "longitude": 44.51,
# #         "start_date": target_date,
# #         "end_date": target_date,
# #         "hourly": "temperature_2m",
# #         "timezone": "auto"
# #     }

# # # extraction
# #     print(f"Fetching weather for ({target_date})...")
# #     response = requests.get(url,params=params)

# # # loading
# #     if response.status_code == 200:
# #         result=response.json()
# #         file_name=f"{target_date}_weather.json"

# #         with open(file_name, "w") as file:
# #             json.dump(result,  file, indent=4)
# #         print(f"✅ Success! Data saved to {file_name}")
# #         return result
# #     else:
# #         print(f"❌ Error: Could not fetch data. Status: {response.status_code}")
# #         return None 

# # specific_date = '2026-03-12'
# # fetch_data = spec_date_weather(specific_date)



# # DATE RANGE WEATHER
# # def get_dates_from_user():
# #     start= input("Enter start date (YYYY-MM-DD) or leave blank for today:")
# #     end= input("Enter end date (YYYY-MM-DD) or leave blank for today:")

# #     if not start:
# #         start=None
# #     if not end:
# #         end=None
    
# #     return start, end


# # def date_ranges_weather(start_date, end_date):
# #     url = "https://api.open-meteo.com/v1/forecast"

# #     params={
# #         "latitude": 40.18,
# #         "longitude": 44.51,
# #         "start_date": start_date,
# #         "end_date": end_date,
# #         "hourly": "temperature_2m",
# #         "timezone": "auto"
# #     }

# #     # # extraction
# #     print(f"Fetching weather for ({start_date}) to ({end_date})...")
# #     response = requests.get(url,params=params)

# #     if response.ok:
# #         result= response.json()
# #         file_name=f"weather_from_{start_date}_to_{end_date}.json"

# #         with open(file_name, "w") as file:
# #             json.dump(result, file, indent=4)
        
# #         print(f"✅ Success! Data saved to {file_name}")
# #         return result
# #     else:
# #         print(f"❌ Error: Could not fetch data. Status: {response.status_code}")
# #         return None 

# # start_date, end_date = get_dates_from_user()
# # range_date = date_ranges_weather(start_date, end_date)


# # WEATHER FOR ALL 3
def fetch_weather_data(lat, lon, start_date=None, end_date=None, city=""):
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
        "timezone": "auto"
    }

    print(f"Fetching weather for ({start_date}) to ({end_date})...")
    response = requests.get(url,params=params)

    if response.ok:
        result= response.json()
        file_name= f"weather_in_{city}_from_{start_date}_to_{end_date}.json"

        with open(file_name, "w") as file:
            json.dump(result, file, indent=4)
        print(f"✅ Success! Data saved to {file_name}")
        return result
    else:
        print(f"❌ Error: Could not fetch data. Status: {response.status_code}")
        return None 


if __name__ == "__main__":
    start= input("Enter start date (YYYY-MM-DD) or leave blank for today:")
    end= input("Enter end date (YYYY-MM-DD) or leave blank for today:")

    weather= fetch_weather_data(52.3676, 4.901,start, end, "Netherland")
   


import requests
import json
import argparse
from datetime import date
from pathlib import Path


# OUTPUT_DIR = Path("/opt/airflow/logs/weather_outputs")



# def fetch_weather_data(lat, lon, start_date=None, end_date=None, city=""):
#     url = "https://api.open-meteo.com/v1/forecast"

#     if not start_date:
#         start_date=date.today().isoformat()
#     if not end_date:
#         end_date=start_date
    
#     params={
#         "latitude": lat,
#         "longitude": lon,
#         "start_date": start_date,
#         "end_date": end_date,
#         "hourly": "temperature_2m",
#         "timezone": "auto"
#     }

#     print(f"Fetching weather for ({start_date}) to ({end_date})...")
#     response = requests.get(url,params=params)

#     if response.ok:
#         result= response.json()
#         file_name= f"weather_in_{city}_from_{start_date}_to_{end_date}.json"
#         # OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
#         # file_path = OUTPUT_DIR / file_name

#         with open(file_path, "w") as file:
#             json.dump(result, file, indent=4)
#         print(f"✅ Success! Data saved to {file}")
#         return result
#     else:
#         print(f"❌ Error: Could not fetch data. Status: {response.status_code}")
#         return None 


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Professional Weather Data Extractor")
    
#     parser.add_argument("--lat", type=float, default=52.3676, help="Latitude")
#     parser.add_argument("--lon", type=float, default=4.901, help="Longitude")
#     parser.add_argument("--city", default="Netherlands", help="City label for output file")
#     parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
#     parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    
#     args = parser.parse_args()

    
#     fetch_weather_data(
#         lat=args.lat,
#         lon=args.lon,
#         city=args.city,
#         start_date=args.start,
#         end_date=args.end,
#     )
   





