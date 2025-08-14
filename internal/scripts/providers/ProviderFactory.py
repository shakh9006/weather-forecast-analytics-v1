import logging

from typing import Dict, Any, List
from .OpenWeatherMapProvider import OpenWeatherMapProvider
from .AccuWeatherProvider import AccuWeatherProvider
from .WeatherBitProvider import WeatherBitProvider
from .TomorrowIOProvider import TomorrowIOProvider
from .ApiNinjasProvider import ApiNinjasProvider
from .base.ProviderAdapter import ProviderAdapter

class ProviderFactory:
    adapters: Dict[str, ProviderAdapter] = {}

    def __init__(self, providers: Dict[str, Dict[str, Any]], countries: List[Dict[str, Any]]):
        logging.info(f"Initializing ProviderFactory with {len(providers)} providers and {len(countries)} countries")
        logging.info(f"Providers: {providers}")
        logging.info(f"Countries: {countries}")

        for provider_name, provider_data in providers.items():
            match provider_name:
                case "openweathermap":
                    self.adapters[provider_name] = OpenWeatherMapProvider(provider_data["api_key"], countries)
                case "accuweather":
                    self.adapters[provider_name] = AccuWeatherProvider(provider_data["api_key"], countries)
                case "weatherbit":
                    self.adapters[provider_name] = WeatherBitProvider(provider_data["api_key"], countries)
                case "tomorrowio":
                    self.adapters[provider_name] = TomorrowIOProvider(provider_data["api_key"], countries)
                case "apininjas":
                    self.adapters[provider_name] = ApiNinjasProvider(provider_data["api_key"], countries)
                case _:
                    logging.error(f"Provider {provider_name} not found")
                    raise ValueError(f"Provider {provider_name} not found")

    def run_provider(self, provider_name: str, start_date: str, end_date: str) -> None:
        logging.info(f"Running provider {provider_name} for {start_date} to {end_date}")
        try:
            for provider in self.adapters:
                if provider_name == provider.name:
                    response_data = provider.fetch_forecast(start_date, end_date)
                    logging.info(f"Response data: {response_data}")
                    break
                else:
                    raise ValueError(f"Provider {provider_name} not found")
        except Exception as e:
            logging.error(f"Error running provider {provider_name}: {e}")
            raise e

    def run_all_providers(self, start_date: str, end_date: str) -> None:
        logging.info(f"Running all providers for {start_date} to {end_date}")
        try:
            for provider_name, provider in self.adapters.items():
                response_data = provider.fetch_forecast(start_date, end_date)
                logging.info(f"Response data for provider {provider_name}: {response_data}")
        except Exception as e:
            logging.error(f"Error running provider {provider_name}: {e}")
            raise e