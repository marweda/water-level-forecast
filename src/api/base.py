"""Utilities for working with HTTP endpoints backed by the `requests` library.

This module contains a minimal `BaseEndpoint` abstraction that issues a single
`GET` request.  Sub-classes are expected to provide a concrete ``url`` attribute
before calling :py:meth:`BaseEndpoint.fetch`.
"""

import requests


class BaseEndpoint:
    """Lightweight wrapper around a single-URL HTTP **GET** request.

    Attributes
    ----------
    url : str
        Absolute URL of the resource to retrieve.
    """

    url: str

    def fetch(self) -> requests.models.Response:
        """Issue a one-off HTTP **GET** request.

        A short-lived :class:`requests.Session` is created to benefit from
        connection pooling without retaining any state between calls.

        Returns
        -------
        requests.models.Response
            The raw response object returned by *requests*.
        """
        with requests.Session() as session:
            return session.get(self.url)
