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