from .client import BaseEndpoint


class DWDEndpoint(BaseEndpoint):
    BASE_URL = "https://opendata.dwd.de/"

    def __init__(self, resource_path: str):
        self.resource_path = resource_path

    @property
    def url(self) -> str:
        return f"{self.BASE_URL}{self.resource_path}"


class DWDPrecipitationHourlyStations(DWDEndpoint):
    def __init__(self):
        resource = "climate_environment/CDC/observations_germany/climate/hourly/precipitation/recent/RR_Stundenwerte_Beschreibung_Stationen.txt"
        super().__init__(resource)


class DWDPrecipitationHourlyData(DWDEndpoint):
    def __init__(self, station_id: str):
        resource = f"climate_environment/CDC/observations_germany/climate/hourly/precipitation/recent/stundenwerte_RR_{station_id}_akt.zip"
        super().__init__(resource)



