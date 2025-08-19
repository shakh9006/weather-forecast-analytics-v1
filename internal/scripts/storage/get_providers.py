import sys

sys.path.append("/opt/airflow/internal")

from project_config.config import OPEN_WEATHER_MAP_API_KEY, WEATHER_BIT_API_KEY

def get_providers():
    return {
        # "openweathermap": {
        #     "provider_name": "OpenWeatherMap",
        #     "api_key": OPEN_WEATHER_MAP_API_KEY,
        # },
        "weatherbit": {
            "provider_name": "WeatherBit",
            "api_key": WEATHER_BIT_API_KEY,
        },
    }
    