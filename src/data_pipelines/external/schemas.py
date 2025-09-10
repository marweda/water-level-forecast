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
    "DWDTemperatureStations",
    "DWDTemperatureMeasurements",
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
    id: Annotated[str, Field(alias="ID", pattern=r".*\d.*")]
    icao: Optional[str] = Field(alias="ICAO")
    name: str = Field(alias="NAME")
    latitude: float = Field(alias="LAT")
    longitude: float = Field(alias="LON")
    elevation: int = Field(alias="ELEV")

    @field_validator("icao", mode="before")
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
    station_id: Annotated[str, Field(alias="Stations_id", min_length=5, max_length=5, pattern=r"^\d{5}$")]
    start_date: datetime = Field(alias="von_datum")
    end_date: datetime = Field(alias="bis_datum")
    station_elevation: int = Field(alias="Stationshoehe")
    latitude: float = Field(alias="geoBreite")
    longitude: float = Field(alias="geoLaenge")
    station_name: str = Field(alias="Stationsname")
    bundesland: str = Field(alias="Bundesland")
    release_status: Optional[str] = Field(alias="Abgabe")

    @field_validator("start_date", "end_date", mode="after")
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


class DWDTemperatureStations(BaseModel):
    """Schema for DWD 10-minute temperature station data.
    
    Similar structure to precipitation stations.
    """
    station_id: Annotated[str, Field(alias="Stations_id", min_length=5, max_length=5, pattern=r"^\d{5}$")]
    start_date: datetime = Field(alias="von_datum")
    end_date: datetime = Field(alias="bis_datum")
    station_elevation: int = Field(alias="Stationshoehe")
    latitude: float = Field(alias="geoBreite")
    longitude: float = Field(alias="geoLaenge")
    station_name: str = Field(alias="Stationsname")
    bundesland: str = Field(alias="Bundesland")
    release_status: Optional[str] = Field(alias="Abgabe")

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def parse_station_date(cls, v: Any) -> datetime:
        """Parse station dates in format YYYYMMDD to UTC-aware datetime."""
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        v_str = str(v).strip()
        return datetime.strptime(v_str, "%Y%m%d").replace(tzinfo=timezone.utc)

    @field_validator("start_date", "end_date", mode="after")
    @classmethod
    def verify_utc(cls, v: datetime) -> datetime:
        """Verify that DWD station dates are in UTC.
        
        DWD data must be in UTC.
        """
        if v.tzinfo != timezone.utc:
            raise ValueError(
                f"DWD temperature station date must be in UTC, got {v.tzinfo}"
            )
        return v

    @model_validator(mode="before")
    @classmethod
    def _clean_empty_strings(cls, data: dict[str, str]) -> dict:
        value = data.get("Abgabe", "")
        data["Abgabe"] = None if value == "-" else value
        return data

    @classmethod
    def validate(
        cls,
        raw: list[dict[str, str]],
    ) -> list[Self]:
        adapter = TypeAdapter(list[cls])
        return adapter.validate_python(raw)


class DWDTemperatureMeasurements(BaseModel):
    """Schema for DWD 10-minute temperature measurement data.
    
    Fields:
    - STATIONS_ID: Station ID
    - MESS_DATUM: Measurement timestamp (YYYYMMDDHHMM format in UTC)
    - QN: Quality note (1, 2, or 3)
    - PP_10: Pressure in hPa (-999 for missing)
    - TT_10: Air temperature at 2m above ground in Celsius (-999 for missing)
    - TM5_10: Air temperature at 5cm above ground in Celsius (-999 for missing)
    - RF_10: Relative humidity in percent (-999 for missing)
    - TD_10: Dew point temperature in Celsius (-999 for missing)
    """
    station_id: str = Field(alias="STATIONS_ID")
    timestamp: datetime = Field(alias="MESS_DATUM")
    quality_note: int = Field(alias="QN")
    pressure_hpa: Optional[float] = Field(alias="PP_10")
    temperature_2m: Optional[float] = Field(alias="TT_10")
    temperature_5cm: Optional[float] = Field(alias="TM5_10")
    relative_humidity: Optional[float] = Field(alias="RF_10")
    dew_point_temperature: Optional[float] = Field(alias="TD_10")

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: str) -> datetime:
        """Parse DWD temperature timestamp and set to UTC.
        
        DWD temperature data timestamps are in UTC but provided as
        naive datetime strings in format YYYYMMDDHHMM.
        """
        return datetime.strptime(v, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)

    @field_validator(
        "pressure_hpa", 
        "temperature_2m", 
        "temperature_5cm", 
        "relative_humidity", 
        "dew_point_temperature", 
        mode="before"
    )
    @classmethod
    def handle_missing_float(cls, v: str) -> Optional[float]:
        """Convert -999 to None for missing values."""
        v_stripped = str(v).strip()
        return None if v_stripped == "-999" else float(v_stripped)

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
        raw: list[dict[str, str]],
        station_id: str,
    ) -> list[Self]:
        adapter = TypeAdapter(list[cls])
        return adapter.validate_python(raw, context={"station_id": station_id})
