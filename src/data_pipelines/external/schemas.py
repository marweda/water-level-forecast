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
    ValidationError,
)

__all__ = [
    "PegelonlineStation",
    "PegelonlineCurrentWaterLevel",
    "PegelonlineForecastedAndEstimatedWaterLevel",
    "DWDMosmixLSingleStationForecasts",
    "DWDMosmixLStations",
    "DWDTenMinNowPercipitationStations",
    "DWDTenMinNowPercipitation",
]


class PegelonlineStation(BaseModel):
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
        raw: dict[str, str],
        uuid: str,
    ) -> Self:
        adapter = TypeAdapter(cls)
        try:
            return adapter.validate_python(raw, context={"uuid": uuid})
        except ValidationError as exc:
            exc.add_note(f"while validating /stations/{uuid}/W/currentmeasurement.json")
            raise


class PegelonlineForecastedAndEstimatedWaterLevel(BaseModel):
    uuid: UUID
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
        raw: list[dict[str, str]],
        uuid: str,
    ) -> list[Self]:
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
        raw: dict[str, str],
        station_id: str,
    ) -> Self:
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
        raw: dict[str, str],
    ) -> Self:
        adapter = TypeAdapter(cls)
        try:
            return adapter.validate_python(raw)
        except ValidationError as exc:
            exc.add_note("While validating MOSMIX_L available stations.")
            raise


class DWDTenMinNowPercipitationStations(BaseModel):
    Stations_id: list[
        Annotated[str, Field(min_length=5, max_length=5, pattern=r"^\d{5}$")]
    ]
    von_datum: list[datetime]
    bis_datum: list[datetime]
    Stationshoehe: list[int]
    geoBreite: list[float]
    geoLaenge: list[float]
    Stationsname: list[str]
    Bundesland: list[str]
    Abgabe: list[Optional[str]]

    @model_validator(mode="before")
    @classmethod
    def _clean_empty_strings(cls, data: dict) -> dict:
        for key, value in data.items():
            if key == "Abgabe":
                data[key] = [None if v == "-" else v for v in value]
        return data

    @classmethod
    def validate(
        cls,
        raw: dict[str, str],
    ) -> Self:
        adapter = TypeAdapter(cls)
        try:
            return adapter.validate_python(raw)
        except ValidationError as exc:
            exc.add_note(
                "While validating DWD available hourly percipitation weather stations."
            )
            raise


class DWDTenMinNowPercipitation(BaseModel):
    station_id: str = Field(alias="STATIONS_ID")
    timestamp: datetime = Field(alias="MESS_DATUM")
    quality_note: int = Field(alias="QN")
    precipitation_duration: Optional[int] = Field(alias="RWS_DAU_10")
    precipitation_amount: Optional[float] = Field(alias="RWS_10")
    precipitation_index: Optional[int] = Field(alias="RWS_IND_10")

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: str) -> datetime:
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
        try:
            return adapter.validate_python(raw, context={"station_id": station_id})
        except ValidationError as exc:
            exc.add_note(
                f"While validating 10min precipitation data for station {station_id}"
            )
            raise
