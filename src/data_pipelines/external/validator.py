from typing import Any

from pydantic import TypeAdapter, ValidationError

from .schemas import (
    PegelonlineStation,
    PegelonlineCurrentWaterLevel,
    PegelonlineForecastedAndEstimatedWaterLevel,
    DWDMosmixLSingleStationForecasts,
)

__all__ = [
    "PegelonlineStationValidator",
    "PegelonlineCurrentWaterLevelValidator",
    "PegelonlineForecastedAndEstimatedWaterLevelValidator",
    "DWDMosmixLSingleStationValidator",
]


class PegelonlineStationValidator:
    _pegelonline_station_adapter = TypeAdapter(list[PegelonlineStation])

    @classmethod
    def validate(
        cls,
        raw: list[dict[str, Any]],
        params: dict[str, Any],
    ) -> list[PegelonlineStation]:
        try:
            return cls._pegelonline_station_adapter.validate_python(raw, context=params)
        except ValidationError as exc:
            exc.add_note(f"while validating /stations.json with params={params!r}")
            raise


class PegelonlineCurrentWaterLevelValidator:
    _adapter = TypeAdapter(PegelonlineCurrentWaterLevel)

    @classmethod
    def validate(cls, raw: dict[str, Any], uuid: str) -> PegelonlineCurrentWaterLevel:
        try:
            return cls._adapter.validate_python(raw, context={"uuid": uuid})
        except ValidationError as exc:
            exc.add_note(f"while validating /stations/{uuid}/W/currentmeasurement.json")
            raise


class PegelonlineForecastedAndEstimatedWaterLevelValidator:
    _adapter = TypeAdapter(list[PegelonlineForecastedAndEstimatedWaterLevel])

    @classmethod
    def validate(
        cls, raw: list[dict[str, Any]], uuid: str
    ) -> list[PegelonlineForecastedAndEstimatedWaterLevel]:
        try:
            return cls._adapter.validate_python(raw, context={"uuid": uuid})
        except ValidationError as exc:
            exc.add_note(f"while validating /stations/{uuid}/WV/measurements.json")
            raise


class DWDMosmixLSingleStationValidator:
    _adapter = TypeAdapter(DWDMosmixLSingleStationForecasts)

    @classmethod
    def validate(cls, raw: dict, station_id: str) -> DWDMosmixLSingleStationForecasts:
        try:
            return cls._adapter.validate_python(raw, context={"station_id": station_id})
        except ValidationError as exc:
            exc.add_note(f"While validating MOSMIX_L forecast for station {station_id}")
            raise
