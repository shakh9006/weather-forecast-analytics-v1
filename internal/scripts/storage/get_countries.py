def get_countries():
    return [
        {
            "country_name": "Japan",
            "city": "Tokyo",
            "country_code": "JP",
            "city_code": "TKO",
            "latitude": 35.6895,
            "longitude": 139.6917,
            "region": "Asia (East)",
        },
        {
            "country_name": "Uzbekistan",
            "city": "Tashkent",
            "country_code": "UZ",
            "city_code": "TAS",
            "latitude": 41.2646,
            "longitude": 69.2163,
            "region": "Asia (Central)",
        },
        {
            "country_name": "Germany",
            "city": "Berlin",
            "country_code": "DE",
            "city_code": "BER",
            "latitude": 52.5200,
            "longitude": 13.4050,
            "region": "Europe",
        },
        {
            "country_name": "United States",
            "city": "New York",
            "country_code": "US",
            "city_code": "NYC",
            "latitude": 40.7128,
            "longitude": -74.0060,
            "region": "North America",
        },
    ]

def get_country_by_location(latitude: float, longitude: float):
    countries = get_countries()
    for country in countries:
        if round(country["latitude"], 2) == round(float(latitude), 2) and round(country["longitude"], 2) == round(float(longitude), 2):
            return country
    return None

def get_country_by_city(city_name: str):
    countries = get_countries()
    for country in countries:
        if country["city"] == city_name:
            return country
    return None