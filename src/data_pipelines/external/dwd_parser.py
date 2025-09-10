import csv
import io
from xml.etree import ElementTree as ET
import zipfile

__all__ = [
    "DWDMosmixLSingleStationKMZParser",
    "DWDMosmixLStationsParser",
    "DWDTenMinNowPercipitationParser",
    "DWDMeasurementStationsParser",
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
    """Parser for DWD MOSMIX-L station catalog."""

    @classmethod
    def _get_column_boundaries(cls, separator_line: str) -> list[tuple[int, int]]:
        """Extract column boundaries from the separator line.

        Args:
            separator_line: Line containing dashes and spaces that define columns

        Returns:
            List of (start, end) tuples for each column
        """
        boundaries = []
        start = None

        for i, char in enumerate(separator_line):
            if char == "-" and start is None:
                start = i
            elif char == " " and start is not None:
                boundaries.append((start, i))
                start = None

        # Handle last column if it extends to the end
        if start is not None:
            boundaries.append((start, len(separator_line)))

        return boundaries

    @classmethod
    def _parse_line(cls, line: str, boundaries: list[tuple[int, int]]) -> list[str]:
        """Parse a single line using column boundaries.

        Args:
            line: Line to parse
            boundaries: List of (start, end) tuples for each column

        Returns:
            List of column values
        """
        values = []
        for start, end in boundaries:
            if len(line) >= end:
                value = line[start:end].strip()
            elif len(line) > start:
                value = line[start:].strip()
            else:
                value = ""
            values.append(value)
        return values

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
        lines = text.splitlines()

        if len(lines) < 3:
            return []

        # Get headers from first line
        header_line = lines[0]
        headers = header_line.split()

        # Get column boundaries from separator line
        separator_line = lines[1]
        boundaries = cls._get_column_boundaries(separator_line)

        # Parse data lines
        data = []
        for line in lines[2:]:
            if line.strip():
                values = cls._parse_line(line, boundaries)
                # Create dict with headers as keys
                row_dict = {}
                for i, header in enumerate(headers):
                    if i < len(values):
                        row_dict[header] = values[i]
                data.append(row_dict)

        return data


class DWDMeasurementStationsParser:
    bundeslaender = {
        "Baden-Württemberg",
        "Berlin",
        "Bremen",
        "Brandenburg",
        "Bayern",
        "Hamburg",
        "Hessen",
        "Mecklenburg-Vorpommern",
        "Niedersachsen",
        "Nordrhein-Westfalen",
        "Rheinland-Pfalz",
        "Saarland",
        "Sachsen",
        "Sachsen-Anhalt",
        "Schleswig-Holstein",
        "Thüringen",
    }

    valid_abgabe_values = {"Frei"}

    @classmethod
    def _extract_txt(cls, stations_content: bytes) -> str:
        """Extract text content from bytes"""
        raw_txt = stations_content.decode("latin-1")
        return raw_txt

    @classmethod
    def _split_by_rows(cls, raw_txt: str) -> list[str]:
        """Split text into rows and clean them"""
        row_splitted_text = raw_txt.split("\n")
        row_splitted_text = [row.strip() for row in row_splitted_text if row.strip()]
        return row_splitted_text

    @classmethod
    def _remove_header_delimiter_row(cls, row_splitted_txt: list[str]) -> list[str]:
        """Remove the delimiter row (second row with dashes)"""
        if len(row_splitted_txt) > 1:
            row_splitted_txt.pop(1)
        if row_splitted_txt and not row_splitted_txt[-1]:
            row_splitted_txt.pop()
        return row_splitted_txt

    @classmethod
    def _parse_station_row(cls, row: str) -> dict[str, str]:
        """Parse a single station row into a dictionary"""
        # Split by spaces to get parts
        parts = row.split()

        # Extract fixed fields from known positions
        stations_id = parts[0]
        von_datum = parts[1]
        bis_datum = parts[2]
        stationshoehe = parts[3]
        geo_breite = parts[4]
        geo_laenge = parts[5]

        # Determine Abgabe value (last part if it's a known value)
        last_part = parts[-1]
        has_abgabe = last_part in cls.valid_abgabe_values

        if has_abgabe:
            abgabe = last_part
            bundesland = parts[-2]
            # Everything between geo_laenge and bundesland is stationsname
            stationsname_parts = parts[6:-2]
        else:
            abgabe = "-"  # Default value when no explicit Abgabe
            bundesland = last_part
            # Everything between geo_laenge and bundesland is stationsname
            stationsname_parts = parts[6:-1]

        stationsname = " ".join(stationsname_parts)

        return {
            "Stations_id": stations_id,
            "von_datum": von_datum,
            "bis_datum": bis_datum,
            "Stationshoehe": stationshoehe,
            "geoBreite": geo_breite,
            "geoLaenge": geo_laenge,
            "Stationsname": stationsname,
            "Bundesland": bundesland,
            "Abgabe": abgabe,
        }

    @classmethod
    def _create_json_structure(cls, filtered_rows: list[str]) -> list[dict[str, str]]:
        stations_data = []
        for row in filtered_rows[1:]:  # Skip header row
            station_dict = cls._parse_station_row(row)
            stations_data.append(station_dict)
        return stations_data

    @classmethod
    def parse(cls, stations_content: bytes) -> list[dict[str, str]]:
        """Parse station content and return list of JSON-like dictionaries"""
        raw_txt = cls._extract_txt(stations_content)
        row_splitted_txt = cls._split_by_rows(raw_txt)
        filtered_rows = cls._remove_header_delimiter_row(row_splitted_txt)
        stations_data = cls._create_json_structure(filtered_rows)
        return stations_data
