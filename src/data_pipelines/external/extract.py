from typing import Type, Optional

from .api import APIEndpoints, BaseURLs, ClientManager
from . import dwd_parser
from . import schemas


__all__ = ["DataExtractor"]



class DataExtractor:
    """Unified data extractor for all external APIs with dependency injection support."""

    def __init__(
        self, 
        client_manager: Optional[ClientManager] = None,
        endpoints: Optional[APIEndpoints] = None
    ):
        """Initialize extractor with injected dependencies.
        
        Args:
            client_manager: Manager for HTTP clients. If None, creates a default one.
            endpoints: API endpoints configuration. If None, uses default endpoints.
        """
        self.client_manager = client_manager or ClientManager()
        self.endpoints = endpoints or APIEndpoints()
        self._owns_client_manager = client_manager is None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close HTTP clients if we own them"""
        if self._owns_client_manager:
            self.client_manager.close()

    def close(self):
        """Manually close HTTP clients if we own them"""
        if self._owns_client_manager:
            self.client_manager.close()

    # Pegelonline methods
    def fetch_pegelonline_stations(
        self, 
        params: dict | None = None,
        schema: Type = schemas.PegelonlineStations
    ) -> list[schemas.PegelonlineStations]:
        """Fetch Pegelonline stations"""
        client = self.client_manager.get("pegelonline")
        response = client.get(self.endpoints.pegelonline_stations, params=params)
        response.raise_for_status()
        raw = response.json()
        return schema.validate(raw, params)

    def fetch_pegelonline_current_water_level(
        self, 
        uuid: str,
        schema: Type = schemas.PegelonlineMeasurements
    ) -> schemas.PegelonlineMeasurements:
        """Fetch current water level for a Pegelonline station"""
        client = self.client_manager.get("pegelonline")
        endpoint = self.endpoints.pegelonline_current_water_level.format(uuid=uuid)
        response = client.get(endpoint)
        response.raise_for_status()
        raw = response.json()
        return schema.validate(raw, uuid)

    def fetch_pegelonline_forecasted_water_level(
        self, 
        uuid: str,
        schema: Type = schemas.PegelonlineForecasts
    ) -> list[schemas.PegelonlineForecasts]:
        """Fetch forecasted and estimated water levels for a Pegelonline station"""
        client = self.client_manager.get("pegelonline")
        endpoint = self.endpoints.pegelonline_forecasted_water_level.format(uuid=uuid)
        response = client.get(endpoint)
        response.raise_for_status()
        raw = response.json()
        return schema.validate(raw, uuid)

    # DWD MOSMIX methods
    def fetch_dwd_mosmix_stations(
        self,
        parser: Type = dwd_parser.DWDMosmixLStationsParser,
        schema: Type = schemas.DWDMosmixLStations
    ) -> list[schemas.DWDMosmixLStations]:
        """Fetch DWD MOSMIX-L station catalog"""
        client = self.client_manager.get("dwd")
        response = client.get(self.endpoints.dwd_mosmix_stations)
        response.raise_for_status()
        
        # Use parser to process the response
        raw_data = parser.parse(response.content)
        return schema.validate(raw_data)

    def fetch_dwd_mosmix_single_station(
        self, 
        station_id: str,
        parser: Type = dwd_parser.DWDMosmixLSingleStationKMZParser,
        schema: Type = schemas.DWDMosmixLForecasts
    ) -> list[schemas.DWDMosmixLForecasts]:
        """Fetch DWD MOSMIX-L forecast for a single station"""
        client = self.client_manager.get("dwd_opendata")
        endpoint = self.endpoints.dwd_mosmix_single_station.format(
            station_id=station_id
        )
        response = client.get(endpoint)
        response.raise_for_status()
        raw_data = parser.parse(response.content)
        return schema.validate(raw_data, station_id)

    # DWD Precipitation methods
    def fetch_dwd_precipitation_stations(
        self,
        parser: Type = dwd_parser.DWDTenMinNowPercipitationStationsParser,
        schema: Type = schemas.DWDPercipitationStations
    ) -> list[schemas.DWDPercipitationStations]:
        """Fetch DWD 10-minute precipitation station catalog"""
        client = self.client_manager.get("dwd_opendata")
        response = client.get(self.endpoints.dwd_precipitation_stations)
        response.raise_for_status()
        
        # Use parser to process the response
        raw_data = parser.parse(response.content)
        return schema.validate(raw_data)
    
    def fetch_raw_test(
        self,
    ) -> bytes:
        """Fetch raw DWD 10-minute precipitation station catalog"""
        client = self.client_manager.get("dwd_opendata")
        response = client.get(self.endpoints.dwd_precipitation_stations)
        return response.content

    def fetch_dwd_precipitation_data(
        self, 
        station_id: str,
        parser: Type = dwd_parser.DWDTenMinNowPercipitationParser,
        schema: Type = schemas.DWDPercipitationMeasurements
    ) -> list[schemas.DWDPercipitationMeasurements]:
        """Fetch DWD 10-minute precipitation data for a station"""
        client = self.client_manager.get("dwd_opendata")
        endpoint = self.endpoints.dwd_precipitation_data.format(station_id=station_id)
        response = client.get(endpoint)
        response.raise_for_status()
        raw_data = parser.parse(response.content)
        return schema.validate(raw_data, station_id)
    
    # DWD Temperature methods
    def fetch_dwd_temperature_stations(
        self,
        parser: Type = dwd_parser.DWDTenMinNowTemperatureStationsParser,
        schema: Type = schemas.DWDTemperatureStations
    ) -> list[schemas.DWDTemperatureStations]:
        """Fetch DWD 10-minute temperature station catalog"""
        client = self.client_manager.get("dwd_opendata")
        response = client.get(self.endpoints.dwd_temperature_stations)
        response.raise_for_status()
        
        # Use parser to process the response
        raw_data = parser.parse(response.content)
        return schema.validate(raw_data)

    def fetch_dwd_temperature_data(
        self, 
        station_id: str,
        parser: Type = dwd_parser.DWDTenMinNowTemperatureParser,
        schema: Type = schemas.DWDTemperatureMeasurements
    ) -> list[schemas.DWDTemperatureMeasurements]:
        """Fetch DWD 10-minute temperature data for a station"""
        client = self.client_manager.get("dwd_opendata")
        endpoint = self.endpoints.dwd_temperature_data.format(station_id=station_id)
        response = client.get(endpoint)
        response.raise_for_status()
        
        # Use parser to process the response
        raw_data = parser.parse(response.content)
        return schema.validate(raw_data, station_id)
