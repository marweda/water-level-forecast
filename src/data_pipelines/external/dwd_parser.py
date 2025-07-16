import csv
import io
from xml.etree import ElementTree as ET
import zipfile

__all__ = [
    "DWDMosmixLSingleStationKMZParser",
    "DWDMosmixLStationsParser",
    "DWDWeatherStationsParser",
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
            if param_name not in ("RR1c", "RR3c"):
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
                "No forecasts found for parameters 'RR1c' or 'RR3c' in the XML content."
            )

        return forecasts

    @classmethod
    def _create_json_structure(
        cls,
        issue_time: str,
        forecast_timestamps: list[str],
        forecasts: dict[str, list[str]],
    ) -> dict[str, str]:
        json_structure = {
            "issue_time": issue_time,
            "timestamps": forecast_timestamps,
            "RR1c": forecasts["RR1c"],
            "RR3c": forecasts["RR3c"],
        }
        return json_structure

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


class DWDMosmixLStationsParser:

    @classmethod
    def _extract_txt(cls, stations_content: bytes) -> str:
        raw_txt = stations_content.decode("latin-1")
        return raw_txt

    @classmethod
    def _split_by_rows(cls, raw_txt: str) -> list[str]:
        row_splitted_text = raw_txt.split("\n")
        return row_splitted_text

    @classmethod
    def _remove_header_deliminator_row(cls, row_splitted_txt: list[str]) -> list[str]:
        row_splitted_txt.pop(1)
        return row_splitted_txt

    @classmethod
    def _extract_columns(cls, parts: list[str]) -> list[str]:
        if len(parts) < 6:
            raise ValueError(f"Expected at least 6 parts, got {len(parts)}: {parts!r}")

        id_, icao = parts[:2]
        lat, lon, elev = parts[-3:]
        name = " ".join(parts[2:-3])
        return [id_, icao, name, lat, lon, elev]

    @classmethod
    def _split_rows_by_columns(cls, row_splitted_txt: list[str]) -> list[list[str]]:
        stations_matrix: list[list[str]] = []
        for row in row_splitted_txt:
            row = row.strip()
            parts = row.split()
            cols = cls._extract_columns(parts)
            stations_matrix.append(cols)
        return stations_matrix

    @classmethod
    def _create_json_structure(
        cls, txt_matrix: list[list[str]]
    ) -> dict[str, list[str]]:
        keys = txt_matrix[0]
        stations_data = dict.fromkeys(keys)
        txt_matrix_without_header = txt_matrix[1:]
        stations_data[keys[0]] = [row[0] for row in txt_matrix_without_header]
        stations_data[keys[1]] = [row[1] for row in txt_matrix_without_header]
        stations_data[keys[2]] = [row[2] for row in txt_matrix_without_header]
        stations_data[keys[3]] = [row[3] for row in txt_matrix_without_header]
        stations_data[keys[4]] = [row[4] for row in txt_matrix_without_header]
        stations_data[keys[5]] = [row[5] for row in txt_matrix_without_header]
        return stations_data

    @classmethod
    def parse(cls, stations_content: bytes) -> dict[str, str]:
        raw_txt = cls._extract_txt(stations_content)
        row_splitted_txt = cls._split_by_rows(raw_txt)
        filtered_row_splitted_txt = cls._remove_header_deliminator_row(row_splitted_txt)
        txt_matrix = cls._split_rows_by_columns(filtered_row_splitted_txt)
        stations_data = cls._create_json_structure(txt_matrix)
        return stations_data


class DWDWeatherStationsParser:

    @classmethod
    def _extract_txt(cls, stations_content: bytes) -> str:
        raw_txt = stations_content.decode("latin-1")
        file_like = io.StringIO(raw_txt)
        reader = csv.reader(file_like, delimiter=";")
        return [row for row in reader]

    @classmethod
    def _create_json_structure(
        cls, txt_matrix: list[list[str]]
    ) -> dict[str, list[str]]:
        keys = [header.replace("-", "") for header in txt_matrix[0]]
        stations_data = dict.fromkeys(keys)
        txt_matrix_without_header = txt_matrix[1:]
        txt_matrix_without_end_control_characters = txt_matrix_without_header[:-1]
        stations_data[keys[0]] = [row[0] for row in txt_matrix_without_end_control_characters]
        stations_data[keys[1]] = [row[1] for row in txt_matrix_without_end_control_characters]
        stations_data[keys[2]] = [row[2] for row in txt_matrix_without_end_control_characters]
        stations_data[keys[3]] = [row[3] for row in txt_matrix_without_end_control_characters]
        stations_data[keys[4]] = [row[4] for row in txt_matrix_without_end_control_characters]
        stations_data[keys[5]] = [row[5] for row in txt_matrix_without_end_control_characters]
        return stations_data

    @classmethod
    def parse(cls, stations_content: bytes) -> dict[str, str]:
        txt_matrix = cls._extract_txt(stations_content)
        stations_data = cls._create_json_structure(txt_matrix)
        return stations_data
