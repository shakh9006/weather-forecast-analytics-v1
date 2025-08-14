from abc import ABC, abstractmethod
from typing import Any, Dict, List

class ProviderAdapter(ABC):
    @abstractmethod
    def fetch_forecast(self, start_date: str, end_date: str) -> List[Any]:
        """Fetch data from provider and return standardized format."""
        pass

    @abstractmethod
    def fetch_current_weather(self, start_date: str, end_date: str) -> List[Any]:
        """Fetch current weather data from provider and return standardized format."""
        pass