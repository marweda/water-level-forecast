from typing import Optional, Dict
from urllib.parse import urlencode

from .base import BaseEndpoint


class PegelonlineEndpoint(BaseEndpoint):
    BASE_URL = "https://www.pegelonline.wsv.de/webservices/rest-api/v2/"

    def __init__(self, resource_path: str, query: Optional[Dict[str, str]] = None):
        self.resource_path = resource_path
        self.query_params = query or {}

    @property
    def url(self) -> str:
        base = f"{self.BASE_URL}{self.resource_path}"
        return f"{base}?{urlencode(self.query_params)}" if self.query_params else base


class PegelonlineStations(PegelonlineEndpoint):
    def __init__(self, filters: Optional[Dict[str, str]] = None):
        super().__init__("stations.json", query=filters)


class PegelonlineWaterLevelCurrent(PegelonlineEndpoint):
    def __init__(self, station_id: str):
        super().__init__(f"{station_id}/W/currentmeasurement.json")


class PegelonlineWaterLevelForecast(PegelonlineEndpoint):
    def __init__(self, station_id: str):
        query_params = {
            "includeTimeseries": "true",
            "hasTimeseries": "WV",
            "includeForecastTimeseries": "true",
        }
        super().__init__(
            f"stations/{station_id}/WV/measurements.json", query=query_params
        )
