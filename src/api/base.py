import requests


class BaseEndpoint:
    def fetch(self) -> requests.Response:
        with requests.Session() as session:
            return session.get(self.url)
