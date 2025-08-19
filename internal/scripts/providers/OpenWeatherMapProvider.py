import requests
import logging
import time

from typing import Dict, Any, List
from .base.ProviderAdapter import ProviderAdapter

from scripts.pandas_process import parse_openweathermap_forecast, parse_openweathermap_current_weather, load_forceast_to_postgres, load_current_weather_to_postgres
from scripts.minio_process import read_from_minio

class OpenWeatherMapProvider(ProviderAdapter):

    def __init__(self, provider_name: str, api_key: str, countries: List[Dict]):
        self.provider_name = provider_name
        self.api_key = api_key
        self.countries = countries

    def fetch_forecast(self, start_date: str, end_date: str) -> List[Any]:
        logging.info(f"Fetching forecast from OpenWeatherMap provider for {start_date} to {end_date}")
        try:
            response_data = []
            api_url = "https://api.openweathermap.org/data/2.5/forecast"

            for country in self.countries:
                params = {
                    "lat": country["latitude"],
                    "lon": country["longitude"],
                    "apikey": self.api_key,
                }

                response = requests.get(api_url, params=params)
                response.raise_for_status()
                data = response.json()
                logging.info(f"Response from OpenWeatherMap provider for country {country['country_name']}")
                response_data.append(data)
                time.sleep(2)
            return response_data
        except Exception as e:
            logging.error(f"Error occurred while fetching forecast from OpenWeatherMap provider: {e}")
            return []

    def fetch_current_weather(self, start_date: str, end_date: str) -> List[Any]:
        logging.info(f"Fetching current weather from OpenWeatherMap provider for {start_date} to {end_date}")
        try:
            response_data = []
            api_url = "https://api.openweathermap.org/data/2.5/weather"
            for country in self.countries:
                params = {
                    "lat": country["latitude"],
                    "lon": country["longitude"],
                    "apikey": self.api_key,
                }

                response = requests.get(api_url, params=params)
                response.raise_for_status()
                data = response.json()
                logging.info(f"Response from OpenWeatherMap provider for country {country['country_name']}")
                response_data.append(data)
                time.sleep(2)
            return response_data
        except Exception as e:
            logging.error(f"Error occurred while fetching current weather from OpenWeatherMap provider: {e}")
            return []

    def process_forecast(self, start_date: str, end_date: str) -> None:
        logging.info(f"Parsing forecast from OpenWeatherMap provider for {start_date} to {end_date}")
        try:
            file_name = f"{self.provider_name}_{start_date}_{end_date}_00-00-00.gz.parquet"
            object_path = f"raw/weather_forecast/{file_name}"
            logging.info(f"Looking for file: {object_path}")
            parquet_data = read_from_minio(object_path, self.provider_name, "weather_forecast")
            df = parse_openweathermap_forecast(parquet_data, start_date)
            load_forceast_to_postgres(df)
            return None
        except Exception as e:
            logging.error(f"Error parsing forecast from OpenWeatherMap provider: {e}")
            return None
        
    def process_current_weather(self, start_date: str, end_date: str) -> None:
        logging.info(f"Parsing current weather from OpenWeatherMap provider for {start_date} to {end_date}")
        try:
            file_name = f"{self.provider_name}_{start_date}_{end_date}_00-00-00.gz.parquet"
            object_path = f"raw/weather_current/{file_name}"
            logging.info(f"Looking for file: {object_path}")
            parquet_data = read_from_minio(object_path, self.provider_name, "weather_current")
            df = parse_openweathermap_current_weather(parquet_data, start_date)
            load_current_weather_to_postgres(df)
            return None
        except Exception as e:
            logging.error(f"Error parsing current weather from OpenWeatherMap provider: {e}")
            return None