import requests
import logging
import time

from typing import Dict, Any, List
from .base.ProviderAdapter import ProviderAdapter

class AccuWeatherProvider(ProviderAdapter):
    def __init__(self, api_key: str, countries: List[Dict]):
        self.api_key = api_key
        self.countries = countries

    def fetch_forecast(self, start_date: str, end_date: str) -> List[Any]:
        logging.info(f"Fetching forecast from AccuWeather provider for {start_date} to {end_date}")
        try:
            response_data = []
            for country in self.countries:
                city_code = self.get_city_code(country["city"])
                api_url = f"https://dataservice.accuweather.com/forecasts/v1/daily/5day/{city_code}"

                params = {
                    "apikey": self.api_key,
                    "details": "true",
                    "metric": "false",
                }

                response = requests.get(api_url, params=params)
                response.raise_for_status()
                data = response.json()
                logging.info(f"Response from AccuWeather provider for country {country['country_name']}")
                response_data.append(data)
                time.sleep(2)
            return response_data
        except Exception as e:
            logging.error(f"Error occurred while fetching forecast from AccuWeather provider: {e}")
            return []

    def fetch_current_weather(self, start_date: str, end_date: str) -> List[Any]:
        logging.info(f"Fetching current weather from AccuWeather provider for {start_date} to {end_date}")
        try:
            response_data = []
            for country in self.countries:
                city_code = self.get_city_code(country["city"])
                api_url = f"https://dataservice.accuweather.com/forecasts/v1/daily/1day/{city_code}"

                params = {
                    "apikey": self.api_key,
                    "details": "true",
                    "metric": "false",
                }

                response = requests.get(api_url, params=params)
                response.raise_for_status()
                data = response.json()
                logging.info(f"Response from AccuWeather provider for country {country['country_name']}")
                response_data.append(data)
                time.sleep(2)
            return response_data
        except Exception as e:
            logging.error(f"Error occurred while fetching current weather from AccuWeather provider: {e}")
            return []

    def get_city_code(self, city_name: str) -> str:
        logging.info(f"Getting city code for {city_name}")
        try:
            code_api_url = "https://dataservice.accuweather.com/locations/v1/cities/search"
            params = {
                "apikey": self.api_key,
                "q": city_name,
            }

            response = requests.get(code_api_url, params=params)
            response.raise_for_status()
            data = response.json()
            logging.info(f"Response from AccuWeather provider for city {city_name}")
            
            if data and len(data) > 0:
                return data[0]["Key"]
            
            logging.error(f"No city code found for {city_name}")
            return ""
        except Exception as e:
            logging.error(f"Error occurred while getting city code for {city_name}: {e}")
            return ""
    