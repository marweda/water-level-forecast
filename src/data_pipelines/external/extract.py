from .util import APIHttpClient
from .dwd_parser import (
    DWDMosmixLSingleStationKMZParser,
    DWDMosmixLStationsParser,
    DWDWeatherStationsParser,
)
from .schemas import (
    PegelonlineStation,
    PegelonlineCurrentWaterLevel,
    PegelonlineForecastedAndEstimatedWaterLevel,
    DWDMosmixLSingleStationForecasts,
    DWDMosmixLStations,
    DWDWeatherStations,
)


__all__ = [
    "PegelonlineStationsExtractor",
    "PegelonlineCurrentWaterLevelExtractor",
    "PegelonlineForecastedAndEstimatedWaterLevelExtractor",
    "DWDMosmixLSingleStationExtractor",
    "DWDMosmixLStationsExtractor",
]


class PegelonlineStationsExtractor:

    @classmethod
    def fetch(
        cls, client: APIHttpClient, params: dict | None = None
    ) -> list[PegelonlineStation]:
        response = client.get("stations.json", params=params)
        response.raise_for_status()
        raw = response.json()
        stations = PegelonlineStation.validate(raw, params)
        return stations


class PegelonlineCurrentWaterLevelExtractor:

    @classmethod
    def fetch(cls, client: APIHttpClient, uuid: str) -> PegelonlineCurrentWaterLevel:
        endpoint = f"stations/{uuid}/W/currentmeasurement.json"
        response = client.get(endpoint)
        response.raise_for_status()
        raw = response.json()
        water_level = PegelonlineCurrentWaterLevel.validate(raw, uuid)
        return water_level


class PegelonlineForecastedAndEstimatedWaterLevelExtractor:

    @classmethod
    def fetch(
        cls, client: APIHttpClient, uuid: str
    ) -> list[PegelonlineForecastedAndEstimatedWaterLevel]:
        endpoint = f"stations/{uuid}/WV/measurements.json"
        response = client.get(endpoint)
        response.raise_for_status()
        raw = response.json()
        measurements = PegelonlineForecastedAndEstimatedWaterLevel.validate(raw, uuid)
        return measurements


class DWDMosmixLSingleStationExtractor:

    @classmethod
    def fetch(
        cls, client: APIHttpClient, station_id: str
    ) -> DWDMosmixLSingleStationForecasts:
        endpoint = (
            f"weather/local_forecasts/mos/MOSMIX_L/single_stations/"
            f"{station_id}/kml/MOSMIX_L_LATEST_{station_id}.kmz"
        )

        response = client.get(endpoint)
        raw_data = DWDMosmixLSingleStationKMZParser.parse(response.content)
        return DWDMosmixLSingleStationForecasts.validate(raw_data, station_id)


class DWDMosmixLStationsExtractor:

    @classmethod
    def fetch(cls, client: APIHttpClient) -> DWDMosmixLStations:
        endpoint = "DE/leistungen/met_verfahren_mosmix/mosmix_stationskatalog.cfg?view=nasPublication"

        response = client.get(endpoint)
        raw_data = DWDMosmixLStationsParser.parse(response.content)
        return DWDMosmixLStations.validate(raw_data)


class DWDWeatherStationsExtractor:

    @classmethod
    def fetch(cls, client: APIHttpClient) -> DWDWeatherStations:
        endpoint = "climate_environment/CDC/help/stations_list_CLIMAT_data.txt"

        response = client.get(endpoint)
        raw_data = DWDWeatherStationsParser.parse(response.content)
        return DWDWeatherStations.validate(raw_data)
