"""Utilities for working with HTTP endpoints backed by the `requests` library.

This module contains a minimal `BaseEndpoint` abstraction that issues a single
`GET` request.  Sub-classes are expected to provide a concrete ``url`` attribute
before calling :py:meth:`BaseEndpoint.fetch`.
"""

import io
from typing import Any, Union
from xml.etree import ElementTree as ET
import zipfile

import httpx

__all__ = ["APIHttpClient", "ClientsBaseURLs", "DWDMosmixLSingleStationKMZParser"]


class ClientsBaseURLs:
    dwd: str = "https://opendata.dwd.de/"
    pegelonline: str = "https://www.pegelonline.wsv.de/webservices/rest-api/v2/"


class APIHttpClient:
    def __init__(self, base_url: str):
        self.client = httpx.Client(base_url=base_url, timeout=60.0)

    def _request(self, method: str, endpoint: str, **kwargs: Any) -> httpx.Response:
        response = self.client.request(method, endpoint, **kwargs)
        return response

    def get(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> httpx.Response:
        return self._request("GET", endpoint, params=params)


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
