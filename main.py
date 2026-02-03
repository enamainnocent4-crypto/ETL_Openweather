
import os
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib.parse import quote_plus

def extract_openweather(lat=0, lon=0):
    load_dotenv("enama.env")

    API_KEY = os.getenv("key")
    if not API_KEY:
        raise ValueError("Clé API OpenWeather introuvable dans enama.env")

    URL = "https://api.openweathermap.org/data/2.5/weather"

    params = {
        "lat": lat,
        "lon": lon,
        "appid": API_KEY,
        "units": "metric",
        "lang": "fr"
    }

    response = requests.get(URL, params=params)

    if response.status_code != 200:
        raise Exception(f"Erreur API OpenWeather : {response.text}")

    df = pd.json_normalize(response.json())
    print("Extraction réussie")

    return df

def transform_openweather(df):
    df = df.copy()

    if "weather" in df.columns:
        df["weather_main"] = df["weather"].str[0].str["main"]
        df["weather_description"] = df["weather"].str[0].str["description"]
        df.drop(columns=["weather"], inplace=True)

    cols_drop = [
        "base", "cod", "id", "timezone",
        "main.sea_level", "main.grnd_level",
        "main.temp_min", "main.temp_max",
        "wind.deg", "wind.gust"
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

    print("Transformation réussie")
    return df

def load_to_mysql(df, table_name="weather_data"):
    load_dotenv("enama.env")

    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")

    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
        raise ValueError("Variables DB manquantes")

    password_encoded = quote_plus(str(DB_PASSWORD))
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{password_encoded}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False
    )

    print("Données chargées dans MySQL")

def run_etl():
    df_raw = extract_openweather(lat=0, lon=0)
    df_clean = transform_openweather(df_raw)
    load_to_mysql(df_clean)


if __name__ == "__main__":
    run_etl()
