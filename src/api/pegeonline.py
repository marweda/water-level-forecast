"""Typed wrappers for the *pegelonline* REST API.

The helpers in this module build on :class:`~.base.BaseEndpoint` and produce
endpoints for the needed *pegelonline* resources.

Each helper only needs the minimum set of parameters required by the
underlying API; all other details such as base URL construction and query-string
encoding are handled internally.

Example
-------
Fetch a list of Rhine gauges that have an active water-level time-series:

```python
from .api import PegelonlineStations

stations = PegelonlineStations(filters={"water": "RHEIN"}).fetch()
stations.raise_for_status()
print(stations.json())
"""

from typing import Optional, Dict
from urllib.parse import urlencode

from .base import BaseEndpoint


class PegelonlineEndpoint(BaseEndpoint):
    """Parameters
    ----------
    resource_path :
        Relative path after ``BASE_URL`` identifying the REST resource
        (e.g. ``"stations.json"`` or ``"stations/{id}/WV/measurements.json"``).
    query :
        Optional mapping that is percent-encoded into the query string.
        Use plain strings for both keys and values; complex types must be
        serialized by the caller.

    Attributes
    ----------
    BASE_URL : str
        Constant prefix for all calls to the v2 API.
    resource_path : str
        The relative URL fragment provided at construction time.
    query_params : Dict[str, str]
        Exact query-string parameters that will be URL-encoded.
    """

    BASE_URL = "https://www.pegelonline.wsv.de/webservices/rest-api/v2/"

    def __init__(self, resource_path: str, query: Optional[Dict[str, str]] = None):
        self.resource_path = resource_path
        self.query_params = query or {}

    @property
    def url(self) -> str:
        """Fully qualified endpoint URL.

        The property concatenates :pyattr:`BASE_URL`, the
        :pyattr:`resource_path`, and an optional query string produced via
        :pyfunc:`urllib.parse.urlencode`.

        Returns
        -------
        str
            Absolute URL that can be passed straight to
            :pymeth:`requests.Session.get`.
        """
        base = f"{self.BASE_URL}{self.resource_path}"
        return f"{base}?{urlencode(self.query_params)}" if self.query_params else base


class PegelonlineStations(PegelonlineEndpoint):
    """List available gauging stations.
    Parameters
    ----------
    filters :
    Arbitrary filter criteria accepted by the `/stations.json` endpoint;
    for example ``{"waters": "RHEIN"}``.
    """

    def __init__(self, filters: Optional[Dict[str, str]] = None):
        super().__init__("stations.json", query=filters)


class PegelonlineWaterLevelCurrent(PegelonlineEndpoint):
    """Retrieve the current water-level measurement for a single station.
        Parameters
    ----------
    station_id :
        The eight-character station identifier used by *pegelonline*.
        Case-insensitive.
    """


def __init__(self, station_id: str):
    super().__init__(f"{station_id}/W/currentmeasurement.json")


class PegelonlineWaterLevelForecast(PegelonlineEndpoint):
    """Retrieve water-level forecast data for a single station.
    The endpoint automatically requests the WV (water level) time-series and
    includes both historic and forecast data in one call.

    Parameters
    ----------
    station_id :
        The uuid station identifier as used by *pegelonline*.

    Notes
    -----
    The query parameters are hard-coded because the API requires them for
    forecast retrieval:

    * ``includeTimeseries=true``
    * ``hasTimeseries=WV``
    * ``includeForecastTimeseries=true``
    """

    def __init__(self, station_id: str):
        query_params = {
            "includeTimeseries": "true",
            "hasTimeseries": "WV",
            "includeForecastTimeseries": "true",
        }
        super().__init__(
            f"stations/{station_id}/WV/measurements.json", query=query_params
        )
