from dataclasses import dataclass

import httpx

from .dwd_parser import (
    DWDMosmixLSingleStationKMZParser,
    DWDMosmixLStationsParser,
    DWDTenMinNowPercipitationStationsParser,
    DWDTenMinNowPercipitationParser,
)
from .schemas import (
    PegelonlineStation,
    PegelonlineCurrentWaterLevel,
    PegelonlineForecastedAndEstimatedWaterLevel,
    DWDMosmixLSingleStationForecasts,
    DWDMosmixLStations,
    DWDTenMinNowPercipitationStations,
    DWDTenMinNowPercipitation,
)


__all__ = ["DataExtractor", "APIEndpoints", "BaseURLs"]


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


class DataExtractor:
    """Unified data extractor for all external APIs"""

    def __init__(self, timeout: float = 60.0):
        """Initialize extractor with HTTP clients for each base URL"""
        self.base_urls = BaseURLs()
        self.endpoints = APIEndpoints()
        self.timeout = timeout

        # Create HTTP clients for each base URL
        self._clients = {
            "dwd_opendata": httpx.Client(
                base_url=self.base_urls.dwd_opendata, timeout=timeout
            ),
            "dwd": httpx.Client(base_url=self.base_urls.dwd, timeout=timeout),
            "pegelonline": httpx.Client(
                base_url=self.base_urls.pegelonline, timeout=timeout
            ),
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close all HTTP clients"""
        for client in self._clients.values():
            client.close()

    def close(self):
        """Manually close all HTTP clients"""
        for client in self._clients.values():
            client.close()

    # Pegelonline methods
    def fetch_pegelonline_stations(
        self, params: dict | None = None
    ) -> list[PegelonlineStation]:
        """Fetch Pegelonline stations"""
        client = self._clients["pegelonline"]
        response = client.get(self.endpoints.pegelonline_stations, params=params)
        response.raise_for_status()
        raw = response.json()
        return PegelonlineStation.validate(raw, params)

    def fetch_pegelonline_current_water_level(
        self, uuid: str
    ) -> PegelonlineCurrentWaterLevel:
        """Fetch current water level for a Pegelonline station"""
        client = self._clients["pegelonline"]
        endpoint = self.endpoints.pegelonline_current_water_level.format(uuid=uuid)
        response = client.get(endpoint)
        response.raise_for_status()
        raw = response.json()
        return PegelonlineCurrentWaterLevel.validate(raw, uuid)

    def fetch_pegelonline_forecasted_water_level(
        self, uuid: str
    ) -> list[PegelonlineForecastedAndEstimatedWaterLevel]:
        """Fetch forecasted and estimated water levels for a Pegelonline station"""
        client = self._clients["pegelonline"]
        endpoint = self.endpoints.pegelonline_forecasted_water_level.format(uuid=uuid)
        response = client.get(endpoint)
        response.raise_for_status()
        raw = response.json()
        return PegelonlineForecastedAndEstimatedWaterLevel.validate(raw, uuid)

    # DWD MOSMIX methods
    def fetch_dwd_mosmix_stations(self) -> list[DWDMosmixLStations]:
        """Fetch DWD MOSMIX-L station catalog"""
        client = self._clients["dwd"]
        response = client.get(self.endpoints.dwd_mosmix_stations)
        response.raise_for_status()
        raw_data = DWDMosmixLStationsParser.parse(response.content)
        return DWDMosmixLStations.validate(raw_data)

    def fetch_dwd_mosmix_single_station(
        self, station_id: str
    ) -> list[DWDMosmixLSingleStationForecasts]:
        """Fetch DWD MOSMIX-L forecast for a single station"""
        client = self._clients["dwd_opendata"]
        endpoint = self.endpoints.dwd_mosmix_single_station.format(
            station_id=station_id
        )
        response = client.get(endpoint)
        response.raise_for_status()
        raw_data = DWDMosmixLSingleStationKMZParser.parse(response.content)
        return DWDMosmixLSingleStationForecasts.validate(raw_data, station_id)

    # DWD Precipitation methods
    def fetch_dwd_precipitation_stations(
        self,
    ) -> list[DWDTenMinNowPercipitationStations]:
        """Fetch DWD 10-minute precipitation station catalog"""
        client = self._clients["dwd_opendata"]
        response = client.get(self.endpoints.dwd_precipitation_stations)
        response.raise_for_status()
        raw_data = DWDTenMinNowPercipitationStationsParser.parse(response.content)
        return DWDTenMinNowPercipitationStations.validate(raw_data)

    def fetch_dwd_precipitation_data(
        self, station_id: str
    ) -> list[DWDTenMinNowPercipitation]:
        """Fetch DWD 10-minute precipitation data for a station"""
        client = self._clients["dwd_opendata"]
        endpoint = self.endpoints.dwd_precipitation_data.format(station_id=station_id)
        response = client.get(endpoint)
        response.raise_for_status()
        raw_data = DWDTenMinNowPercipitationParser.parse(response.content)
        return DWDTenMinNowPercipitation.validate(raw_data, station_id)
