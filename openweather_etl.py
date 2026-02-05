import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from datetime import datetime
import pytz

VILLES_FRANCE = [
    {"city": "Paris", "lat": 48.8566, "lon": 2.3522},
    {"city": "Marseille", "lat": 43.2965, "lon": 5.3698},
    {"city": "Lyon", "lat": 45.7640, "lon": 4.8357},
    {"city": "Toulouse", "lat": 43.6047, "lon": 1.4442},
    {"city": "Nice", "lat": 43.7102, "lon": 7.2620},
    {"city": "Nantes", "lat": 47.2184, "lon": -1.5536},
    {"city": "Strasbourg", "lat": 48.5734, "lon": 7.7521},
    {"city": "Montpellier", "lat": 43.6108, "lon": 3.8767},
    {"city": "Bordeaux", "lat": 44.8378, "lon": -0.5792},
    {"city": "Lille", "lat": 50.6292, "lon": 3.0573},
]

def extract_openweather(lat, lon):
    load_dotenv("enama.env")
    api_key = os.getenv("key")

    if not api_key:
        raise ValueError("Clé API OpenWeather manquante")

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "metric",
        "lang": "fr"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    return pd.json_normalize(response.json())

def transform_openweather(df, city_name):
    df = df.copy()

    df["weather_main"] = df["weather"].str[0].str["main"]
    df["description"] = df["weather"].str[0].str["description"]

    df.rename(columns={
        "coord.lon": "Longitude",
        "coord.lat": "Latitude",
        "main.temp": "temperature",
        "main.feels_like": "feels_like",
        "main.pressure": "pressure",
        "main.humidity": "humidity",
        "wind.speed": "wind_speed",
        "clouds.all": "cloudiness",
    }, inplace=True)

    paris_tz = pytz.timezone("Europe/Paris")
    df["datetime"] = datetime.now(paris_tz)
    df["city"] = city_name

    return df[[
        "city", "datetime", "Longitude", "Latitude",
        "temperature", "feels_like", "pressure", "humidity",
        "wind_speed", "cloudiness", "weather_main", "description"
    ]]

def load_to_mysql(df, table_name="weather_data"):
    load_dotenv("enama.env")

    engine = create_engine(
        f"mysql+pymysql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD'))}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

    df.to_sql(
        table_name,
        con=engine,
        if_exists="append",
        index=False
    )

    print("Les Données sont insérées dans MySQL")

def run_etl():
    dfs = []

    for ville in VILLES_FRANCE:
        df_raw = extract_openweather(ville["lat"], ville["lon"])
        df_clean = transform_openweather(df_raw, ville["city"])
        dfs.append(df_clean)

    df_final = pd.concat(dfs, ignore_index=True)
    load_to_mysql(df_final)

    return df_final

if __name__ == "__main__":
    INTERVAL = 300
    print("ETL démarré pour 10 grandes villes françaises (toutes les 2 minutes)")

    while True:
        start_time = time.time()

        try:
            df = run_etl()
            print(df)
        except Exception as e:
            print("Erreur ETL :", e)

        time.sleep(max(0, INTERVAL - (time.time() - start_time)))
