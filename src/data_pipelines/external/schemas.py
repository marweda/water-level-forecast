from datetime import datetime
from typing import Literal, Optional, Any
from typing_extensions import Annotated

from uuid import UUID

from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
    ValidationInfo,
    TypeAdapter,
    ValidationError,
)

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

    @classmethod
    def validate(
        cls,
        raw: list[dict[str, Any]],
        params: dict[str, Any],
    ) -> list["PegelonlineStation"]:
        adapter = TypeAdapter(list[cls])
        try:
            return adapter.validate_python(raw, context=params)
        except ValidationError as exc:
            exc.add_note(f"while validating /stations.json with params={params!r}")
            raise


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

    @classmethod
    def validate(
        cls,
        raw: dict[str, Any],
        uuid: str,
    ) -> "PegelonlineCurrentWaterLevel":
        adapter = TypeAdapter(cls)
        try:
            return adapter.validate_python(raw, context={"uuid": uuid})
        except ValidationError as exc:
            exc.add_note(f"while validating /stations/{uuid}/W/currentmeasurement.json")
            raise


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

    @classmethod
    def validate(
        cls,
        raw: list[dict[str, Any]],
        uuid: str,
    ) -> list["PegelonlineForecastedAndEstimatedWaterLevel"]:
        adapter = TypeAdapter(list[cls])
        try:
            return adapter.validate_python(raw, context={"uuid": uuid})
        except ValidationError as exc:
            exc.add_note(f"while validating /stations/{uuid}/WV/measurements.json")
            raise


class DWDMosmixLSingleStationForecasts(BaseModel):
    station_id: str  # To be injected via validator
    issue_time: datetime
    timestamps: list[datetime]
    RR1c: list[Optional[float]]
    RR3c: list[Optional[float]]

    @model_validator(mode="before")
    @classmethod
    def convert_values_and_timestamps(cls, data: dict, info: ValidationInfo) -> dict:
        # Convert ISO strings to datetime
        if "issue_time" in data and isinstance(data["issue_time"], str):
            try:
                data["issue_time"] = datetime.fromisoformat(
                    data["issue_time"].replace("Z", "+00:00")
                )
            except ValueError as err:
                raise ValueError(f"Invalid issue_time: {data['issue_time']!r}") from err

        if "timestamps" in data and isinstance(data["timestamps"], list):
            converted = []
            for ts in data["timestamps"]:
                try:
                    converted.append(datetime.fromisoformat(ts.replace("Z", "+00:00")))
                except ValueError as err:
                    raise ValueError(f"Invalid timestamp: {ts!r}") from err
            data["timestamps"] = converted

        for param in ("RR1c", "RR3c"):
            vals = []
            for v in data[param]:
                vals.append(None if v == "-" else v)
            data[param] = vals

        if info.context and "station_id" in info.context:
            data["station_id"] = info.context["station_id"]

        return data

    @classmethod
    def validate(
        cls,
        raw: dict[str, Any],
        station_id: str,
    ) -> "DWDMosmixLSingleStationForecasts":
        adapter = TypeAdapter(cls)
        try:
            return adapter.validate_python(raw, context={"station_id": station_id})
        except ValidationError as exc:
            exc.add_note(f"While validating MOSMIX_L forecast for station {station_id}")
            raise


class DWDMosmixLStations(BaseModel):
    ID: list[Annotated[str, Field(pattern=r".*\d.*")]]
    ICAO: list[str]
    NAME: list[str]
    LAT: list[float]
    LON: list[float]
    ELEV: list[int]

    @classmethod
    def validate(
        cls,
        raw: dict[str, Any],
    ) -> "DWDMosmixLStations":
        adapter = TypeAdapter(cls)
        try:
            return adapter.validate_python(raw)
        except ValidationError as exc:
            exc.add_note("While validating MOSMIX_L available stations.")
            raise


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

    @classmethod
    def validate(
        cls,
        raw: dict[str, Any],
    ) -> "DWDWeatherStations":
        adapter = TypeAdapter(cls)
        try:
            return adapter.validate_python(raw)
        except ValidationError as exc:
            exc.add_note("While validating DWD available weather stations.")
            raise
