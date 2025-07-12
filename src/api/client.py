"""Utilities for working with HTTP endpoints backed by the `requests` library.

This module contains a minimal `BaseEndpoint` abstraction that issues a single
`GET` request.  Sub-classes are expected to provide a concrete ``url`` attribute
before calling :py:meth:`BaseEndpoint.fetch`.
"""

from typing import Any, dict, NamedTuple

import httpx


class ClientsBaseURLS(NamedTuple):
    dwd: str = "https://opendata.dwd.de/"
    pegelonline: str = "https://www.pegelonline.wsv.de/webservices/rest-api/v2/"


class APIHttpClient:
    def __init__(self, base_url: str):
        self.client = httpx.Client(base_url=base_url)

    def request(self, method: str, endpoint: str, **kwargs: Any) -> httpx.Response:
        response = self.client.request(method, endpoint, **kwargs)
        response.raise_for_status()
        return response

    def get(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> httpx.Response:
        return self.request("GET", endpoint, params=params)
