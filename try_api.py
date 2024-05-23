import datetime as dt
import requests
import pandas as pd
from sqlalchemy import create_engine
from pandas import json_normalize

# OpenWeatherMap API details
BASE_URL = f"http://api.openweathermap.org/data/2.5/forecast?"
API_KEY = "062267bfd5742d16ec5087c8a6e966a9"
CITY = "Hyderabad"
LAT = "17.3850"  # Latitude of Hyderabad
LON = "78.4867"  # Longitude of Hyderabad

# Construct the API request URL
url = f"{BASE_URL}lat={LAT}&lon={LON}&appid={API_KEY}"
print(url)
# Make the API request
response = requests.get(url).json()
print(response)

# Extract relevant data from the response and create DataFrame
data_list = response['list']
formatted_data = []

for item in data_list:
    formatted_data.append({
        'datetime': dt.datetime.utcfromtimestamp(item['dt']),
        'temperature': item['main']['temp'],
        'feels_like': item['main']['feels_like'],
        'description': item['weather'][0]['description'],
        'cloudiness': item['clouds']['all'],
        'wind_speed': item['wind']['speed'],
        'wind_direction': item['wind']['deg']
    })

# Create a DataFrame from the formatted data
df = pd.DataFrame(formatted_data)
df1 = pd.DataFrame(data_list)
print(df1)
engine = create_engine('postgresql://postgres:Postgres@localhost/demo')

df.to_sql('API_DATA', engine, if_exists='replace', index=False)
print(df)
