from dataclasses import dataclass
from typing import Dict

import httpx

__all__ = [
    "BaseURLs",
    "APIEndpoints",
    "ClientManager",
]


@dataclass(frozen=True)
class BaseURLs:
    """Base URLs for different data providers"""

    dwd_opendata: str = "https://opendata.dwd.de/"
    dwd: str = "https://www.dwd.de/"
    pegelonline: str = "https://www.pegelonline.wsv.de/webservices/rest-api/v2/"


@dataclass(frozen=True)
class APIEndpoints:
    """Endpoint configurations for all APIs"""

    # Pegelonline endpoints
    pegelonline_stations: str = "stations.json"
    pegelonline_current_water_level: str = "stations/{uuid}/W/currentmeasurement.json"
    pegelonline_forecasted_water_level: str = "stations/{uuid}/WV/measurements.json"

    # DWD MOSMIX endpoints
    dwd_mosmix_stations: str = (
        "DE/leistungen/met_verfahren_mosmix/mosmix_stationskatalog.cfg?view=nasPublication"
    )
    dwd_mosmix_single_station: str = (
        "weather/local_forecasts/mos/MOSMIX_L/single_stations/{station_id}/kml/MOSMIX_L_LATEST_{station_id}.kmz"
    )

    # DWD precipitation endpoints
    dwd_precipitation_stations: str = (
        "climate_environment/CDC/observations_germany/climate/10_minutes/precipitation/now/zehn_now_rr_Beschreibung_Stationen.txt"
    )
    dwd_precipitation_data: str = (
        "climate_environment/CDC/observations_germany/climate/10_minutes/precipitation/now/10minutenwerte_nieder_{station_id}_now.zip"
    )

    # DWD temperature endpoints
    dwd_temperature_stations: str = (
        "climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/now/zehn_now_tu_Beschreibung_Stationen.txt"
    )
    dwd_temperature_data: str = (
        "climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/now/10minutenwerte_TU_{station_id}_now.zip"
    )


class ClientManager:
    """Manages reusable HTTPX clients for each base URL."""

    def __init__(self, base_urls: BaseURLs | None = None, timeout: float = 60.0):
        self.base_urls = base_urls or BaseURLs()
        self.timeout = timeout
        self._clients: Dict[str, httpx.Client] = {
            "dwd_opendata": httpx.Client(
                base_url=self.base_urls.dwd_opendata, timeout=timeout
            ),
            "dwd": httpx.Client(base_url=self.base_urls.dwd, timeout=timeout),
            "pegelonline": httpx.Client(
                base_url=self.base_urls.pegelonline, timeout=timeout
            ),
        }

    def get(self, name: str) -> httpx.Client:
        try:
            return self._clients[name]
        except KeyError as e:
            raise KeyError(f"Unknown client name: {name}") from e

    def close(self):
        for client in self._clients.values():
            client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

