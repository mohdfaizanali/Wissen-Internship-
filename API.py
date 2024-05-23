# import datetime as dt
# import requests
# import pandas as pd
# from sqlalchemy import create_engine
# import pytz
#
# # OpenWeatherMap API details
# BASE_URL = f"http://api.openweathermap.org/data/2.5/forecast?"
# API_KEY = "062267bfd5742d16ec5087c8a6e966a9"
# CITY = "Hyderabad"
# LAT = "17.3850"  # Latitude of Hyderabad
# LON = "78.4867"  # Longitude of Hyderabad
#
# # Construct the API request URL
# url = f"{BASE_URL}lat={LAT}&lon={LON}&appid={API_KEY}"
#
# # Make the API request
# response = requests.get(url).json()
#
# print(response)
#
# # Extract relevant data and create DataFrame
# data = []
# for forecast in response['list']:
#     timestamp_utc = dt.datetime.fromtimestamp(forecast['dt'], tz=pytz.UTC)
#     data.append({
#         'timestamp': timestamp_utc,
#         'temp': forecast['main']['temp'],
#         'feels_like': forecast['main']['feels_like'],
#         'temp_min': forecast['main']['temp_min'],
#         'temp_max': forecast['main']['temp_max'],
#         'pressure': forecast['main']['pressure'],
#         'humidity': forecast['main']['humidity'],
#         'weather_id': forecast['weather'][0]['id'],
#         'weather_main': forecast['weather'][0]['main'],
#         'weather_description': forecast['weather'][0]['description'],
#         'weather_icon': forecast['weather'][0]['icon'],
#         'clouds_all': forecast['clouds']['all'],
#         'wind_speed': forecast['wind']['speed'],
#         'wind_deg': forecast['wind']['deg']
#     })
#
# df = pd.DataFrame(data)
#
# # Connect to PostgreSQL
# # Replace 'your_username', 'your_password', 'your_host', and 'your_database' with your PostgreSQL credentials
# engine = create_engine('postgresql://postgres:Postgres@localhost/demo')
#
# # Insert DataFrame into PostgreSQL
# df.to_sql('weather_data1', engine, if_exists='replace', index=False)
# print(df)
n = int(input("Enter your number:"))
# result = num * i
i = 1
while i <=10 :
    print(n * i)
    i += 1
