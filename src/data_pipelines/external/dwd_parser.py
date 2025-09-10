import csv
import io
from xml.etree import ElementTree as ET
import zipfile

import pandas as pd

__all__ = [
    "DWDMosmixLSingleStationKMZParser",
    "DWDMosmixLStationsParser",
    "DWDTenMinNowPercipitationStationsParser",
    "DWDTenMinNowPercipitationParser",
    "DWDTenMinNowTemperatureStationsParser",
    "DWDTenMinNowTemperatureParser",
]


class DWDMosmixLSingleStationKMZParser:
    """Parser for DWD MOSMIX-L single station KMZ forecasts (raw values)"""

    # XML namespaces
    NS = {
        "kml": "http://www.opengis.net/kml/2.2",
        "dwd": "https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd",
    }

    @classmethod
    def _extract_kml(cls, kmz_content: bytes) -> str:
        """Extract KML content from KMZ archive"""
        with io.BytesIO(kmz_content) as bio:
            with zipfile.ZipFile(bio) as kmz:
                if not kmz.filelist:
                    raise ValueError("Empty KMZ archive")
                with kmz.open(kmz.filelist[0]) as kml_file:
                    return kml_file.read().decode("ISO-8859-1")

    @classmethod
    def _parse_issue_time(cls, root: ET.Element) -> str:
        """Parse issue time."""
        issue_time_elem = root.find(".//dwd:IssueTime", cls.NS)
        if issue_time_elem is not None and issue_time_elem.text:
            issue_time = issue_time_elem.text.strip()
        else:
            raise ValueError(
                "Missing or empty <IssueTime> element in DWD MOSMIX L XML."
            )
        return issue_time

    @classmethod
    def _parse_forecast_timestamps(cls, root: ET.Element) -> list[str]:
        """Parse forecast timestamps"""
        timestamps = []
        timesteps_elem = root.find(".//dwd:ForecastTimeSteps", cls.NS)
        if timesteps_elem is not None:
            for ts_elem in timesteps_elem.findall("dwd:TimeStep", cls.NS):
                if ts_elem.text:
                    timestamps.append(ts_elem.text.strip())
                else:
                    raise ValueError(
                        "Encountered <TimeStep> element with no text content in <ForecastTimeSteps>."
                    )
        return timestamps

    @classmethod
    def _parse_forecasts(cls, root: ET.Element) -> dict[str, list[str]]:
        """Parse forecasts."""
        forecasts: dict[str, list[str]] = {}

        placemark = root.find(".//kml:Placemark", cls.NS)
        if placemark is None:
            raise ValueError("Missing <kml:Placemark> element in the XML structure.")

        for forecast_elem in placemark.findall(".//dwd:Forecast", cls.NS):
            param_name = forecast_elem.attrib.get(f"{{{cls.NS['dwd']}}}elementName")
            if param_name not in ("RR1c", "RR3c", "TTT"):
                continue

            value_elem = forecast_elem.find("dwd:value", cls.NS)
            if value_elem is None or value_elem.text is None:
                raise ValueError(
                    f"Missing <value> element or text for forecast parameter '{param_name}'."
                )

            # Split values, because values are within one whitespace separated string
            values = [v.strip() for v in value_elem.text.split()]
            forecasts[param_name] = values

        # Ensure at least one relevant parameter was parsed
        if not forecasts:
            raise ValueError(
                "No forecasts found for parameters 'RR1c', 'RR3c', or 'TTT' in the XML content."
            )

        return forecasts

    @classmethod
    def _create_json_structure(
        cls,
        issue_time: str,
        forecast_timestamps: list[str],
        forecasts: dict[str, list[str]],
    ) -> dict[str, str]:
        rr1c_values = forecasts.get("RR1c", ["-"] * len(forecast_timestamps))
        rr3c_values = forecasts.get("RR3c", ["-"] * len(forecast_timestamps))
        ttt_values = forecasts.get("TTT", ["-"] * len(forecast_timestamps))
        return [
            {
                "issue_time": issue_time,
                "timestamp": ft,
                "RR1c": rv1,
                "RR3c": rv3,
                "TTT": ttt,
            }
            for ft, rv1, rv3, ttt in zip(
                forecast_timestamps, rr1c_values, rr3c_values, ttt_values
            )
        ]

    @classmethod
    def parse(cls, kmz_content: bytes) -> dict[str, str]:
        """Main parsing method"""
        kml_content = cls._extract_kml(kmz_content)
        root = ET.fromstring(kml_content)

        issue_time = cls._parse_issue_time(root)
        forecast_timestamps = cls._parse_forecast_timestamps(root)
        forecasts = cls._parse_forecasts(root)

        # Prepare forecasts structure
        forecast_data = cls._create_json_structure(
            issue_time, forecast_timestamps, forecasts
        )
        return forecast_data


class DWDTenMinNowPercipitationParser:

    @classmethod
    def parse(cls, zip_content: bytes) -> list[dict]:
        # Extract TXT file from ZIP
        with io.BytesIO(zip_content) as bio:
            with zipfile.ZipFile(bio) as zip_ref:
                txt_files = [f for f in zip_ref.namelist() if f.endswith(".txt")]
                if not txt_files:
                    raise ValueError("No TXT file found in ZIP archive")

                with zip_ref.open(txt_files[0]) as txt_file:
                    content = txt_file.read().decode("latin-1")

        # Parse CSV content
        reader = csv.DictReader(
            io.StringIO(content), delimiter=";", skipinitialspace=True
        )

        data = []
        for row in reader:
            if "eor" in row:  # Remove end-of-row marker
                row.pop("eor")
            data.append(row)

        return data


class DWDTenMinNowTemperatureParser:
    """Parser for DWD 10-minute temperature data from ZIP files."""

    @classmethod
    def parse(cls, zip_content: bytes) -> list[dict]:
        # Extract TXT file from ZIP
        with io.BytesIO(zip_content) as bio:
            with zipfile.ZipFile(bio) as zip_ref:
                txt_files = [f for f in zip_ref.namelist() if f.endswith(".txt")]
                if not txt_files:
                    raise ValueError("No TXT file found in temperature ZIP archive")

                with zip_ref.open(txt_files[0]) as txt_file:
                    content = txt_file.read().decode("latin-1")

        # Parse CSV content with semicolon delimiter
        reader = csv.DictReader(
            io.StringIO(content), delimiter=";", skipinitialspace=True
        )

        data = []
        for row in reader:
            # Remove end-of-row marker and empty keys
            if "eor" in row:
                row.pop("eor")
            # Remove any empty string keys that might result from trailing semicolons
            row = {k: v for k, v in row.items() if k}
            data.append(row)

        return data


class DWDMosmixLStationsParser:
    """Parser for DWD MOSMIX-L station catalog using pandas."""

    @classmethod
    def parse(cls, content: bytes) -> list[dict]:
        """Parse DWD MOSMIX-L station catalog.
        
        Args:
            content: Raw bytes from the stations file
            
        Returns:
            List of station dictionaries
        """
        # Decode the content
        text = content.decode("latin-1")
        buffer = io.StringIO(text)
        
        # Read using pandas read_fwf, skip the separator line
        df = pd.read_fwf(buffer, skiprows=[1])
        
        # Convert to list of dicts
        return df.to_dict(orient="records")


class DWDTenMinNowPercipitationStationsParser:
    """Parser for DWD 10-minute precipitation station catalog using pandas."""

    @classmethod
    def parse(cls, content: bytes) -> list[dict]:
        """Parse DWD precipitation station catalog.
        
        Args:
            content: Raw bytes from the stations file
            
        Returns:
            List of station dictionaries with properly parsed fields
        """
        # Decode the content
        text = content.decode("latin-1")
        buffer = io.StringIO(text)
        
        # Read using pandas read_fwf, skip the separator line
        df = pd.read_fwf(buffer, skiprows=[1])
        
        # Convert to list of dicts
        return df.to_dict(orient="records")


class DWDTenMinNowTemperatureStationsParser:
    """Parser for DWD 10-minute temperature station catalog using pandas."""

    @classmethod
    def parse(cls, content: bytes) -> list[dict]:
        """Parse DWD temperature station catalog.
        
        Args:
            content: Raw bytes from the stations file
            
        Returns:
            List of station dictionaries with properly parsed fields
        """
        # Decode the content
        text = content.decode("latin-1")
        buffer = io.StringIO(text)
        
        # Read using pandas read_fwf, skip the separator line
        df = pd.read_fwf(buffer, skiprows=[1])
        
        # Convert to list of dicts
        return df.to_dict(orient="records")
