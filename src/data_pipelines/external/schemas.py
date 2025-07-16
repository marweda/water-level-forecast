from datetime import datetime
from typing import Literal, Optional
from typing_extensions import Annotated

from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator, ValidationInfo


__all__ = [
    "PegelonlineStation",
    "PegelonlineCurrentWaterLevel",
    "PegelonlineForecastedAndEstimatedWaterLevel",
    "DWDMosmixLSingleStationForecasts",
    "DWDMosmixLStations",
    "DWDWeatherStations",
]


class PegelonlineStation(BaseModel):
    uuid: UUID
    number: int
    shortname: str
    longname: str
    km: float
    agency: str
    longitude: float
    latitude: float
    water: dict[str, str]

    @field_validator("water", mode="after")
    @classmethod
    def ensure_correct_water(
        cls,
        value: dict[str, str],
        info: ValidationInfo,
    ) -> dict[str, str]:
        ctx = info.context
        if isinstance(ctx, dict) and "waters" in ctx:
            allowed = set(ctx["waters"])
            if value.get("longname") not in allowed:
                raise ValueError(
                    f"Station water {value} not among requested waters={allowed}"
                )
        return value


class PegelonlineCurrentWaterLevel(BaseModel):
    uuid: UUID  # To be injected via validator
    timestamp: datetime
    value: int
    stateMnwMhw: str
    stateNswHsw: str

    @model_validator(mode="before")
    @classmethod
    def inject_uuid(cls, data, info: ValidationInfo):
        uuid = info.context.get("uuid")
        if uuid is not None:
            data["uuid"] = uuid
        return data


class PegelonlineForecastedAndEstimatedWaterLevel(BaseModel):
    uuid: UUID  # To be injected via validator
    initialized: datetime
    timestamp: datetime
    value: int
    type: Literal["forecast", "estimate"]

    @model_validator(mode="before")
    @classmethod
    def inject_uuid(cls, data, info: ValidationInfo):
        uuid = info.context.get("uuid")
        if uuid is not None:
            data["uuid"] = uuid
        return data


class DWDMosmixLSingleStationForecasts(BaseModel):
    station_id: str  # To be injected via validator
    issue_time: datetime
    timestamps: list[datetime]
    RR1c: list[Optional[float]]
    RR3c: list[Optional[float]]

    @model_validator(mode="before")
    @classmethod
    def convert_values_and_timestamps(cls, data: dict, info: ValidationInfo) -> dict:
        if "issue_time" in data and isinstance(data["issue_time"], str):
            try:
                # Handle ISO format with Z timezone
                data["issue_time"] = datetime.fromisoformat(
                    data["issue_time"].replace("Z", "+00:00")
                )
            except ValueError as err:
                raise ValueError(f"Invalid issue_time: {data["issue_time"]!r}") from err

        if "timestamps" in data and isinstance(data["timestamps"], list):
            converted_timestamps = []
            for ts in data["timestamps"]:
                try:
                    converted_timestamps.append(
                        datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    )
                except ValueError as err:
                    raise ValueError(f"Invalid timestamp: {ts!r}") from err
            data["timestamps"] = converted_timestamps

        for param in ("RR1c", "RR3c"):
            converted_values = []
            for value in data[param]:
                if value == "-":
                    converted_values.append(None)
                else:
                    converted_values.append(value)

            data[param] = converted_values

        if info.context and "station_id" in info.context:
            data["station_id"] = info.context["station_id"]

        return data


class DWDMosmixLStations(BaseModel):
    ID: list[Annotated[str, Field(pattern=r".*\d.*")]]
    ICAO: list[str]
    NAME: list[str]
    LAT: list[float]
    LON: list[float]
    ELEV: list[int]


class DWDWeatherStations(BaseModel):
    WMOStationID: list[
        Annotated[str, Field(min_length=5, max_length=5, pattern=r"^\d{5}$")]
    ]
    StationName: list[Optional[str]]
    Latitude: list[Optional[float]]
    Longitude: list[Optional[float]]
    Height: list[Optional[int]]
    Country: list[Optional[str]]

    @model_validator(mode="before")
    @classmethod
    def _clean_empty_strings(cls, data: dict) -> dict:
        for key, value in data.items():
            if key != "WMOStationID":
                data[key] = [None if v == "" else v for v in value]
        return data
