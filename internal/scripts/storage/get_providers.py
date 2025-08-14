def get_providers():
    """
        Mp88VEYJ1gE4vkS1gt23KQ==fKvvb7b69korttTS

        https://api.api-ninjas.com/v1/weatherforecast?city=London

        "location": "{city_code}",
        "timesteps": "1d",
        "apikey": "{API key}",
    """
    return {
        "openweathermap": {
            "provider_name": "OpenWeatherMap",
            "api_key": "3bda580d6ee8a9aed19d1c4b1e0578db",
        },
        "accuweather": {
            "provider_name": "AccuWeather",
            "api_key": "h4fxOBLhc20UnOuYtq2AAdzCERrfsCqh",
        },
        "weatherbit": {
            "provider_name": "WeatherBit",
            "api_key": "44da1e2a84474c839cda4390805e00ab",
        },
        "tomorrowio": {
            "provider_name": "TomorrowIO",
            "api_key": "poQonEqbXdnof2EXxQvScAZ1ty3YLAEo",
        },
        "apininjas": {
            "provider_name": "API Ninjas",
            "api_key": "Mp88VEYJ1gE4vkS1gt23KQ==fKvvb7b69korttTS",
        },
    }