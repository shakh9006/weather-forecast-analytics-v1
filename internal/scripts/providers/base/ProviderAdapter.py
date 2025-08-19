from abc import ABC, abstractmethod
from typing import Any, Dict, List
import pandas as pd

class ProviderAdapter(ABC):
    @abstractmethod
    def fetch_forecast(self, start_date: str, end_date: str) -> List[Any]:
        """Fetch data from provider and return standardized format."""
        pass

    @abstractmethod
    def fetch_current_weather(self, start_date: str, end_date: str) -> List[Any]:
        """Fetch current weather data from provider and return standardized format."""
        pass

    @abstractmethod
    def process_forecast(self, start_date: str, end_date: str) -> None:
        """Process forecast data from provider."""
        pass

    @abstractmethod
    def process_current_weather(self, start_date: str, end_date: str) -> None:
        """Process current weather data from provider."""
        pass