import os
import requests
import pandas as pd
from dotenv import load_dotenv
 
load_dotenv("enama.env")  

API_KEY = os.getenv("key")
print(API_KEY)
if not API_KEY:
    raise ValueError("Clé API OpenWeather introuvable dans enama.env")

URL = "https://api.openweathermap.org/data/2.5/weather"

params = {
    "lat": 0,
    "lon": 0,
    "appid": API_KEY,
    "units": "metric",
    "lang": "fr"
}

response = requests.get(URL, params=params)
data = response.json()
print(response.status_code)
print(data)

if response.status_code != 200:
    print("Erreur API :", data)
else:
    
    df = pd.json_normalize(data)
    print(df)
    
    
    
def transform_openweather(df):
    df = df.copy()
    if "weather" in df.columns:
        df["weather_main"] = df["weather"].str[0].str["main"]
        df["weather_description"] = df["weather"].str[0].str["description"]

        df.drop(columns=["weather"], inplace=True)
        
    cols_drop = [
        "base",
        "cod",
        "id",
        "timezone",
        "main.sea_level",
        "main.grnd_level",
        "main.temp_min",
        "main.temp_max",
        "wind.deg",
        "wind.gust"
    ]
    df.drop(columns=[c for c in cols_drop if c in df.columns], inplace=True)
    df.rename(columns={
        "coord.lon": "Longitude",
        "coord.lat": "Latitude",
        "main.humidity": "humidity",
        "main.pressure": "pressure",
        "clouds.all": "cloudiness",
        "dt": "datetime",
        "wind.speed": "wind_speed",
        "name": "city",
        "main.temp": "temperature",
        "main.feels_like": "feels_like",
        "visibility": "visibility",
        "weather_description": "description",
        "sys.sunrise": "sunrise",
        "sys.sunset": "sunset",
    }, inplace=True)

    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], unit="s")

    return df
df_clean = transform_openweather(df)
print(df_clean)