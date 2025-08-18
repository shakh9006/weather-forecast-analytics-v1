import requests
import logging
import time

from typing import Dict, Any, List
from .base.ProviderAdapter import ProviderAdapter

class TomorrowIOProvider(ProviderAdapter):
    def __init__(self, provider_name: str, api_key: str, countries: List[Dict]):
        self.provider_name = provider_name
        self.api_key = api_key
        self.countries = countries

    def fetch_forecast(self, start_date: str, end_date: str) -> List[Any]:
        logging.info(f"Fetching forecast from TomorrowIO provider for {start_date} to {end_date}")
        try:
            response_data = []
            api_url = "https://api.tomorrow.io/v4/weather/forecast"

            for country in self.countries:
                params = {
                    "location": country["city"],
                    "timesteps": "5d",
                    "apikey": self.api_key,
                }

                response = requests.get(api_url, params=params)
                response.raise_for_status()
                data = response.json()
                logging.info(f"Response from TomorrowIO provider for country {country['country_name']}")
                response_data.append(data)
                time.sleep(2)
            return response_data
        except Exception as e:
            logging.error(f"Error occurred while fetching forecast from TomorrowIO provider: {e}")
            return []

    def fetch_current_weather(self, start_date: str, end_date: str) -> List[Any]:
        logging.info(f"Fetching current weather from TomorrowIO provider for {start_date} to {end_date}")
        try:
            response_data = []
            api_url = "https://api.tomorrow.io/v4/weather/forecast"

            for country in self.countries:
                params = {
                    "location": country["city"],
                    "timesteps": "1d",
                    "apikey": self.api_key,
                }

                response = requests.get(api_url, params=params)
                response.raise_for_status()
                data = response.json()
                logging.info(f"Response from TomorrowIO provider for country {country['country_name']}")
                response_data.append(data)
                time.sleep(2)
            return response_data
        except Exception as e:
            logging.error(f"Error occurred while fetching current weather from TomorrowIO provider: {e}")
            return []

    def process_forecast(self, start_date: str, end_date: str) -> None:
        logging.info(f"Processing forecast from TomorrowIO provider for {start_date} to {end_date}")
        try:
            return None
        except Exception as e:
            logging.error(f"Error processing forecast from TomorrowIO provider: {e}")
            return None