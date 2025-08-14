from datetime import datetime, timezone
from typing import Literal, Optional, Any, Self
from typing_extensions import Annotated

from uuid import UUID

from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
    ValidationInfo,
    TypeAdapter,
)

__all__ = [
    "PegelonlineStations",
    "PegelonlineMeasurements",
    "PegelonlineForecasts",
    "DWDMosmixLForecasts",
    "DWDMosmixLStations",
    "DWDPercipitationStations",
    "DWDPercipitationMeasurements",
]


class PegelonlineStations(BaseModel):
    uuid: UUID
    number: int
    shortname: str
    longname: str
    km: float
    agency: str
    longitude: Optional[float]
    latitude: Optional[float]
    water: dict[str, str]

    @model_validator(mode="before")
    @classmethod
    def ensure_location_keys(cls, data):
        if "longitude" not in data:
            data["longitude"] = None
        if "latitude" not in data:
            data["latitude"] = None
        return data

    @field_validator("water", mode="after")
    @classmethod
    def ensure_correct_water(
        cls,
        value: dict[str, str],
        info: ValidationInfo,
    ) -> dict[str, str]:
        ctx = info.context
        if isinstance(ctx, dict) and "waters" in ctx:
            allowed = ctx["waters"]
            if value.get("longname") not in allowed:
                raise ValueError(
                    f"Station water {value} not among requested waters={allowed}"
                )
        return value

    @classmethod
    def validate(
        cls,
        raw: list[dict[str, str]],
        params: dict[str, str],
    ) -> list[Self]:
        adapter = TypeAdapter(list[cls])
        return adapter.validate_python(raw, context=params)


class PegelonlineMeasurements(BaseModel):
    uuid: UUID  # To be injected via validator
    timestamp: datetime
    value: int
    stateMnwMhw: str
    stateNswHsw: str

    @field_validator("timestamp", mode="after")
    @classmethod
    def convert_to_utc(cls, v: datetime) -> datetime:
        """Convert Pegelonline timestamp to UTC.
        
        Pegelonline provides timestamps with timezone offset (e.g., +01:00, +02:00).
        """
        return v.astimezone(timezone.utc)

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
        raw: dict[str, str],
        uuid: str,
    ) -> Self:
        adapter = TypeAdapter(cls)
        return adapter.validate_python(raw, context={"uuid": uuid})


class PegelonlineForecasts(BaseModel):
    uuid: UUID
    initialized: datetime
    timestamp: datetime
    value: int
    type: Literal["forecast", "estimate"]

    @field_validator("initialized", "timestamp", mode="after")
    @classmethod
    def convert_to_utc(cls, v: datetime) -> datetime:
        """Convert Pegelonline timestamps to UTC.
        
        Pegelonline provides timestamps with timezone offset (e.g., +01:00, +02:00).
        """
        return v.astimezone(timezone.utc)

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
        raw: list[dict[str, str]],
        uuid: str,
    ) -> list[Self]:
        adapter = TypeAdapter(list[cls])
        return adapter.validate_python(raw, context={"uuid": uuid})


class DWDMosmixLForecasts(BaseModel):
    station_id: str  # To be injected via validator
    issue_time: datetime
    timestamp: datetime
    RR1c: Optional[float]
    RR3c: Optional[float]
    TTT: Optional[float]  # Temperature in Kelvin

    @model_validator(mode="before")
    @classmethod
    def convert_values_and_timestamps(
        cls, data: dict, info: ValidationInfo
    ) -> list[dict]:
        # Convert ISO strings to datetime
        # DWD MOSMIX data comes with 'Z' suffix indicating UTC
        try:
            data["issue_time"] = datetime.fromisoformat(
                data["issue_time"].replace("Z", "+00:00")
            )
        except ValueError as err:
            raise ValueError(f"Invalid issue_time: {data['issue_time']!r}") from err

        try:
            data["timestamp"] = datetime.fromisoformat(
                data["timestamp"].replace("Z", "+00:00")
            )
        except ValueError as err:
            raise ValueError(f"Invalid timestamp: {data["timestamp"]!r}") from err

        for param in ("RR1c", "RR3c", "TTT"):
            value = data[param]
            data[param] = None if value == "-" else value

        if info.context and "station_id" in info.context:
            data["station_id"] = info.context["station_id"]

        return data

    @field_validator("issue_time", "timestamp", mode="after")
    @classmethod
    def verify_utc(cls, v: datetime) -> datetime:
        """Verify that DWD timestamps are in UTC.
        
        DWD MOSMIX data must be in UTC (provided with 'Z' suffix).
        """
        if v.tzinfo != timezone.utc:
            raise ValueError(
                f"DWD MOSMIX timestamp must be in UTC, got {v.tzinfo}"
            )
        return v

    @classmethod
    def validate(
        cls,
        raw: list[dict[str, str]],
        station_id: str,
    ) -> Self:
        adapter = TypeAdapter(list[cls])
        return adapter.validate_python(raw, context={"station_id": station_id})


class DWDMosmixLStations(BaseModel):
    ID: Annotated[str, Field(pattern=r".*\d.*")]
    ICAO: Optional[str]
    NAME: str
    LAT: float
    LON: float
    ELEV: int

    @field_validator("ICAO", mode="before")
    @classmethod
    def _dash_means_none(cls, v):
        return None if v == "----" else v

    @classmethod
    def validate(
        cls,
        raw: list[dict[str, str]],
    ) -> Self:
        adapter = TypeAdapter(list[cls])
        return adapter.validate_python(raw)


class DWDPercipitationStations(BaseModel):
    Stations_id: Annotated[str, Field(min_length=5, max_length=5, pattern=r"^\d{5}$")]
    von_datum: datetime
    bis_datum: datetime
    Stationshoehe: int
    geoBreite: float
    geoLaenge: float
    Stationsname: str
    Bundesland: str
    Abgabe: Optional[str]

    @field_validator("von_datum", "bis_datum", mode="after")
    @classmethod
    def verify_utc(cls, v: datetime) -> datetime:
        """Verify that DWD station dates are in UTC.
        
        DWD data must be in UTC.
        """
        if v.tzinfo != timezone.utc:
            raise ValueError(
                f"DWD precipitation station date must be in UTC, got {v.tzinfo}"
            )
        return v

    @model_validator(mode="before")
    @classmethod
    def _clean_empty_strings(cls, data: dict[str, str]) -> dict:
        value = data["Abgabe"]
        data["Abgabe"] = None if value == "-" else value
        return data

    @classmethod
    def validate(
        cls,
        raw: list[dict[str, str]],
    ) -> Self:
        adapter = TypeAdapter(list[cls])
        return adapter.validate_python(raw)


class DWDPercipitationMeasurements(BaseModel):
    station_id: str = Field(alias="STATIONS_ID")
    timestamp: datetime = Field(alias="MESS_DATUM")
    quality_note: int = Field(alias="QN")
    precipitation_duration: Optional[int] = Field(alias="RWS_DAU_10")
    precipitation_amount: Optional[float] = Field(alias="RWS_10")
    precipitation_index: Optional[int] = Field(alias="RWS_IND_10")

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: str) -> datetime:
        """Parse DWD precipitation timestamp and set to UTC.
        
        DWD precipitation data timestamps are in UTC but provided as
        naive datetime strings in format YYYYMMDDHHMM.
        """
        return datetime.strptime(v, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)

    @field_validator("precipitation_duration", "precipitation_index", mode="before")
    @classmethod
    def handle_missing_int(cls, v: str) -> Optional[int]:
        return None if v.strip() == "-999" else int(v)

    @field_validator("precipitation_amount", mode="before")
    @classmethod
    def handle_missing_float(cls, v: str) -> Optional[float]:
        return None if v.strip() == "-999" else float(v)

    @model_validator(mode="before")
    @classmethod
    def process_station_data(cls, data: dict, info: ValidationInfo) -> dict:
        # Pad STATIONS_ID to 5 digits with leading zeros
        station_id_str = str(data["STATIONS_ID"])
        data["STATIONS_ID"] = station_id_str.zfill(5)

        context = info.context or {}
        expected = context.get("station_id")
        if expected and str(data.get("STATIONS_ID")) != expected:
            raise ValueError(
                f"Station ID mismatch: expected {expected}, "
                f"got {data.get('STATIONS_ID')}"
            )
        return data

    @classmethod
    def validate(
        cls,
        raw: dict[str, str],
        station_id: str,
    ) -> list[Self]:
        adapter = TypeAdapter(list[cls])
        return adapter.validate_python(raw, context={"station_id": station_id})
