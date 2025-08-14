from abc import ABC, abstractmethod
from typing import Any, Dict, List

class ProviderAdapter(ABC):
    @abstractmethod
    def fetch_forecast(self, request: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch data from provider and return standardized format."""
        pass

    @abstractmethod
    def fetch_current_weather(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch current weather data from provider and return standardized format."""
        pass