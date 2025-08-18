import logging
import sys

sys.path.append("/opt/airflow/internal")

from scripts.pandas_process import process_data_with_pandas
from scripts.minio_process import save_to_minio

from typing import Dict, Any, List, Optional
from .OpenWeatherMapProvider import OpenWeatherMapProvider
from .WeatherBitProvider import WeatherBitProvider
from .TomorrowIOProvider import TomorrowIOProvider
from .ApiNinjasProvider import ApiNinjasProvider
from .base.ProviderAdapter import ProviderAdapter

class ProviderFactory:
    adapters: Dict[str, ProviderAdapter] = {}

    def __init__(self, providers: Dict[str, Dict[str, Any]], countries: Optional[List[Dict[str, Any]]] = None):
        logging.info(f"Initializing ProviderFactory with {len(providers)} providers")

        if countries is not None:
            logging.info(f"And with {len(countries)} countries")

        for provider_name, provider_data in providers.items():
            match provider_name:
                case "openweathermap":
                    self.adapters[provider_name] = OpenWeatherMapProvider(provider_name, provider_data["api_key"], countries)
                case "weatherbit":
                    self.adapters[provider_name] = WeatherBitProvider(provider_name, provider_data["api_key"], countries)
                case "tomorrowio":
                    self.adapters[provider_name] = TomorrowIOProvider(provider_name, provider_data["api_key"], countries)
                case "apininjas":
                    self.adapters[provider_name] = ApiNinjasProvider(provider_name, provider_data["api_key"], countries)
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
                # Fetch forecast data
                response_data = provider.fetch_forecast(start_date, end_date)
                logging.info(f"Response data for provider {provider_name}")
                df = process_data_with_pandas(response_data)
                save_to_minio(df, start_date, end_date, provider_name, "weather_forecast")

                # Fetch current weather data
                response_data = provider.fetch_current_weather(start_date, end_date)
                logging.info(f"Response data for provider {provider_name}")
                df = process_data_with_pandas(response_data)
                save_to_minio(df, start_date, end_date, provider_name, "weather_current")
        except Exception as e:
            logging.error(f"Error running provider {provider_name}: {e}")
            raise e
        
    def load_forecast_from_s3_to_pg(self, start_date: str, end_date: str) -> None:
        logging.info(f"Loading forecast data from S3 to PG for {start_date} to {end_date}")
        try:
            for provider_name, provider in self.adapters.items():
                logging.info(f"Processing forecast data for provider {provider_name}")
                provider.process_forecast(start_date, end_date)
        except Exception as e:
            logging.error(f"Error loading data from S3 to PG: {e}")
            raise e