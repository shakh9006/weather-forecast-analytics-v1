import requests
import logging

from typing import Dict, Any, List
from .base.ProviderAdapter import ProviderAdapter

class OpenWeatherMapProvider(ProviderAdapter):

    """
    Example: https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={API key}&units=metric&lang=en
    """

    def __init__(self, api_key: str, countries: List[Dict]):
        self.api_key = api_key
        self.countries = countries

    def fetch_forecast(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        logging.info(f"Fetching forecast from OpenWeatherMap provider for {start_date} to {end_date}")
        try:
            response_data = []
            api_url = "https://api.openweathermap.org/data/2.5/forecast"

            for country in self.countries:
                params = {
                    "lat": country["latitude"],
                    "lon": country["longitude"],
                    "apikey": self.api_key,
                    "units": "metric",
                    "lang": "en",
                }

                response = requests.get(api_url, params=params)
                response.raise_for_status()
                data = response.json()
                logging.info(f"Response from OpenWeatherMap provider for country {country['country_name']}")
                response_data.append(data)

            return response_data
        except Exception as e:
            logging.error(f"Error occurred while fetching forecast from OpenWeatherMap provider: {e}")
            return []

    def fetch_current_weather(self, request: Dict[str, Any]) -> Dict[str, Any]:
        pass

    