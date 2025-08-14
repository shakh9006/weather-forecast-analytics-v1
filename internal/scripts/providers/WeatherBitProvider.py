import requests
import logging

from typing import Dict, Any, List
from .base.ProviderAdapter import ProviderAdapter

class WeatherBitProvider(ProviderAdapter):

    """
    Example: https://api.weatherbit.io/v2.0/forecast/daily?city={city}&key={API key}
    """

    def __init__(self, api_key: str, countries: List[Dict]):
        self.api_key = api_key
        self.countries = countries

    def fetch_forecast(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        logging.info(f"Fetching forecast from WeatherBitProvider provider for {start_date} to {end_date}")
        try:
            response_data = []
            api_url = "https://api.weatherbit.io/v2.0/forecast/daily"

            for country in self.countries:
                params = {
                    "key": self.api_key,
                    "city": country["city"],
                }

                response = requests.get(api_url, params=params)
                response.raise_for_status()
                data = response.json()
                logging.info(f"Response from WeatherBitProvider provider for country {country['country_name']}")
                response_data.append(data)

            return response_data
        except Exception as e:
            logging.error(f"Error occurred while fetching forecast from WeatherBitProvider provider: {e}")
            return []

    def fetch_current_weather(self, request: Dict[str, Any]) -> Dict[str, Any]:
        pass

    