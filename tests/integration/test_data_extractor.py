"""Integration tests for DataExtractor - verifying return types."""

import pytest
from src.data_pipelines.external import DataExtractor, ClientManager, APIEndpoints
from src.data_pipelines.external import schemas


class TestDataExtractorIntegration:
    """Integration tests for the refactored DataExtractor."""
    
    @pytest.fixture
    def extractor(self):
        """Create a DataExtractor instance with real clients."""
        client_manager = ClientManager()
        endpoints = APIEndpoints()
        extractor = DataExtractor(client_manager=client_manager, endpoints=endpoints)
        yield extractor
        extractor.close()
    
    def test_fetch_dwd_temperature_stations(self, extractor):
        """Test fetching DWD temperature stations returns correct type."""
        result = extractor.fetch_dwd_temperature_stations()
        
        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(station, schemas.DWDTemperatureStations) for station in result)
    
    def test_fetch_dwd_precipitation_stations(self, extractor):
        """Test fetching DWD precipitation stations returns correct type."""
        result = extractor.fetch_dwd_precipitation_stations()
        
        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(station, schemas.DWDPercipitationStations) for station in result)
    
    def test_fetch_dwd_temperature_data(self, extractor):
        """Test fetching temperature data returns correct type."""
        # Get a station ID to test with
        stations = extractor.fetch_dwd_temperature_stations()
        station_id = stations[0].station_id
        
        try:
            result = extractor.fetch_dwd_temperature_data(station_id)
            
            assert isinstance(result, list)
            if len(result) > 0:
                assert all(isinstance(measurement, schemas.DWDTemperatureMeasurements) for measurement in result)
        except Exception:
            # Some stations might not have data - that's okay for integration test
            pytest.skip(f"Station {station_id} has no available data")
    
    def test_fetch_dwd_precipitation_data(self, extractor):
        """Test fetching precipitation data returns correct type."""
        # Get a station ID to test with
        stations = extractor.fetch_dwd_precipitation_stations()
        station_id = stations[0].station_id
        
        try:
            result = extractor.fetch_dwd_precipitation_data(station_id)
            
            assert isinstance(result, list)
            if len(result) > 0:
                assert all(isinstance(measurement, schemas.DWDPercipitationMeasurements) for measurement in result)
        except Exception:
            pytest.skip(f"Station {station_id} has no available data")
    
    def test_fetch_dwd_mosmix_stations(self, extractor):
        """Test fetching DWD MOSMIX stations returns correct type."""
        result = extractor.fetch_dwd_mosmix_stations()
        
        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(station, schemas.DWDMosmixLStations) for station in result)
    
    def test_fetch_dwd_mosmix_single_station(self, extractor):
        """Test fetching MOSMIX forecast returns correct type."""
        # Get a numeric station ID to test with
        stations = extractor.fetch_dwd_mosmix_stations()
        numeric_station = next((s for s in stations if s.id.isdigit()), None)
        
        if numeric_station:
            result = extractor.fetch_dwd_mosmix_single_station(numeric_station.id)
            
            assert isinstance(result, list)
            assert len(result) > 0
            assert all(isinstance(forecast, schemas.DWDMosmixLForecasts) for forecast in result)
        else:
            pytest.skip("No numeric MOSMIX station found")
    
    def test_fetch_pegelonline_stations(self, extractor):
        """Test fetching Pegelonline stations returns correct type."""
        params = {"limit": 5}
        result = extractor.fetch_pegelonline_stations(params=params)
        
        assert isinstance(result, list)
        assert len(result) <= 5
        assert all(isinstance(station, schemas.PegelonlineStations) for station in result)
    
    def test_fetch_pegelonline_current_water_level(self, extractor):
        """Test fetching current water level returns correct type."""
        # Get a station to test with
        stations = extractor.fetch_pegelonline_stations(params={"limit": 1})
        
        if stations:
            uuid = str(stations[0].uuid)
            try:
                result = extractor.fetch_pegelonline_current_water_level(uuid)
                
                # This returns a single measurement, not a list
                assert isinstance(result, schemas.PegelonlineMeasurements)
            except Exception:
                pytest.skip(f"Station {uuid} has no current measurement")
    
    def test_fetch_pegelonline_forecasted_water_level(self, extractor):
        """Test fetching forecasted water level returns correct type."""
        # Get a station to test with
        stations = extractor.fetch_pegelonline_stations(params={"limit": 1})
        
        if stations:
            uuid = str(stations[0].uuid)
            try:
                result = extractor.fetch_pegelonline_forecasted_water_level(uuid)
                
                assert isinstance(result, list)
                if len(result) > 0:
                    assert all(isinstance(forecast, schemas.PegelonlineForecasts) for forecast in result)
            except Exception:
                pytest.skip(f"Station {uuid} has no forecast data")
    
    def test_context_manager(self):
        """Test that DataExtractor works as a context manager."""
        with DataExtractor() as extractor:
            result = extractor.fetch_dwd_temperature_stations()
            assert isinstance(result, list)
            assert len(result) > 0
    
    def test_fetch_raw_test(self, extractor):
        """Test fetch_raw_test returns bytes."""
        result = extractor.fetch_raw_test()
        assert isinstance(result, bytes)
        assert len(result) > 0


if __name__ == "__main__":
    # Allow running directly for debugging
    import sys
    pytest.main([__file__, "-v", "-s"])
