#!/usr/bin/env python3
"""
Test script to verify that extractors work with updated UTC handling.
"""

from datetime import timezone
from src.data_pipelines.external.extract import DataExtractor


def test_pegelonline_extractors():
    """Test Pegelonline extractors with UTC conversion."""
    print("\n" + "="*60)
    print("Testing Pegelonline Extractors")
    print("="*60)
    
    with DataExtractor() as extractor:
        # Test 1: Fetch stations
        print("\n1. Fetching Pegelonline stations...")
        try:
            stations = extractor.fetch_pegelonline_stations(
                params={"limit": 2, "waters": "RHEIN,DONAU"}
            )
            print(f"   ✓ Found {len(stations)} stations")
            if stations:
                station = stations[0]
                print(f"   Station example: {station.shortname} ({station.uuid})")
        except Exception as e:
            print(f"   ✗ Error fetching stations: {e}")
            return False
        
        # Test 2: Fetch current water level
        if stations:
            print(f"\n2. Fetching current water level for {station.shortname}...")
            try:
                measurement = extractor.fetch_pegelonline_current_water_level(
                    str(station.uuid)
                )
                print(f"   ✓ Got measurement at {measurement.timestamp}")
                print(f"   Timestamp timezone: {measurement.timestamp.tzinfo}")
                
                # Verify UTC conversion
                if measurement.timestamp.tzinfo != timezone.utc:
                    print(f"   ✗ ERROR: Timestamp not in UTC! Got {measurement.timestamp.tzinfo}")
                    return False
                else:
                    print(f"   ✓ Timestamp correctly converted to UTC")
                    
            except Exception as e:
                print(f"   ✗ Error fetching measurement: {e}")
                # Not all stations have current measurements, continue
        
        # Test 3: Try to fetch forecasted water level (may not exist for all stations)
        if stations:
            print(f"\n3. Attempting to fetch forecasted water level for {station.shortname}...")
            try:
                forecasts = extractor.fetch_pegelonline_forecasted_water_level(
                    str(station.uuid)
                )
                if forecasts:
                    print(f"   ✓ Got {len(forecasts)} forecast entries")
                    forecast = forecasts[0]
                    print(f"   First forecast timestamp: {forecast.timestamp}")
                    print(f"   Timestamp timezone: {forecast.timestamp.tzinfo}")
                    
                    # Verify UTC conversion
                    if forecast.timestamp.tzinfo != timezone.utc:
                        print(f"   ✗ ERROR: Forecast timestamp not in UTC!")
                        return False
                    if forecast.initialized.tzinfo != timezone.utc:
                        print(f"   ✗ ERROR: Forecast initialized timestamp not in UTC!")
                        return False
                    print(f"   ✓ Forecast timestamps correctly converted to UTC")
            except Exception as e:
                print(f"   ⚠ No forecast data available (this is normal): {e}")
    
    return True


def test_dwd_mosmix_extractors():
    """Test DWD MOSMIX extractors with UTC verification."""
    print("\n" + "="*60)
    print("Testing DWD MOSMIX Extractors")
    print("="*60)
    
    with DataExtractor() as extractor:
        # Test 1: Fetch MOSMIX stations
        print("\n1. Fetching DWD MOSMIX stations...")
        try:
            stations = extractor.fetch_dwd_mosmix_stations()
            print(f"   ✓ Found {len(stations)} stations")
            if stations:
                # Find a station with numeric ID for forecast test
                station = next((s for s in stations if s.ID.isdigit()), stations[0])
                print(f"   Station example: {station.NAME} (ID: {station.ID})")
        except Exception as e:
            print(f"   ✗ Error fetching stations: {e}")
            return False
        
        # Test 2: Fetch MOSMIX forecast for a single station
        if stations and station.ID.isdigit():
            print(f"\n2. Fetching MOSMIX forecast for station {station.ID}...")
            try:
                forecasts = extractor.fetch_dwd_mosmix_single_station(station.ID)
                print(f"   ✓ Got {len(forecasts)} forecast entries")
                if forecasts:
                    forecast = forecasts[0]
                    print(f"   Issue time: {forecast.issue_time}")
                    print(f"   First timestamp: {forecast.timestamp}")
                    print(f"   Issue time timezone: {forecast.issue_time.tzinfo}")
                    print(f"   Timestamp timezone: {forecast.timestamp.tzinfo}")
                    
                    # Verify UTC
                    if forecast.issue_time.tzinfo != timezone.utc:
                        print(f"   ✗ ERROR: Issue time not in UTC!")
                        return False
                    if forecast.timestamp.tzinfo != timezone.utc:
                        print(f"   ✗ ERROR: Timestamp not in UTC!")
                        return False
                    print(f"   ✓ All timestamps verified as UTC")
                    
            except Exception as e:
                print(f"   ✗ Error fetching forecast: {e}")
                return False
    
    return True


def test_dwd_precipitation_extractors():
    """Test DWD precipitation extractors with UTC verification."""
    print("\n" + "="*60)
    print("Testing DWD Precipitation Extractors")
    print("="*60)
    
    with DataExtractor() as extractor:
        # Test 1: Fetch precipitation stations
        print("\n1. Fetching DWD precipitation stations...")
        try:
            stations = extractor.fetch_dwd_precipitation_stations()
            print(f"   ✓ Found {len(stations)} stations")
            if stations:
                station = stations[0]
                print(f"   Station example: {station.Stationsname} (ID: {station.Stations_id})")
                print(f"   Date range: {station.von_datum} to {station.bis_datum}")
                print(f"   von_datum timezone: {station.von_datum.tzinfo}")
                print(f"   bis_datum timezone: {station.bis_datum.tzinfo}")
                
                # Verify UTC
                if station.von_datum.tzinfo != timezone.utc:
                    print(f"   ✗ ERROR: von_datum not in UTC!")
                    return False
                if station.bis_datum.tzinfo != timezone.utc:
                    print(f"   ✗ ERROR: bis_datum not in UTC!")
                    return False
                print(f"   ✓ Station dates verified as UTC")
                
        except Exception as e:
            print(f"   ✗ Error fetching stations: {e}")
            return False
        
        # Test 2: Fetch precipitation data for a station
        if stations:
            print(f"\n2. Fetching precipitation data for station {station.Stations_id}...")
            try:
                measurements = extractor.fetch_dwd_precipitation_data(station.Stations_id)
                print(f"   ✓ Got {len(measurements)} measurements")
                if measurements:
                    measurement = measurements[0]
                    print(f"   First measurement timestamp: {measurement.timestamp}")
                    print(f"   Timestamp timezone: {measurement.timestamp.tzinfo}")
                    
                    # Verify UTC
                    if measurement.timestamp.tzinfo != timezone.utc:
                        print(f"   ✗ ERROR: Measurement timestamp not in UTC!")
                        return False
                    print(f"   ✓ Measurement timestamp verified as UTC")
                    
            except Exception as e:
                print(f"   ✗ Error fetching measurements: {e}")
                # Some stations might not have recent data
                print(f"   ⚠ This might be normal if station has no recent data")
    
    return True


def main():
    """Run all extractor tests."""
    print("\n" + "="*60)
    print("TESTING DATA EXTRACTORS WITH UTC DATETIME HANDLING")
    print("="*60)
    
    results = {
        "Pegelonline": test_pegelonline_extractors(),
        "DWD MOSMIX": test_dwd_mosmix_extractors(),
        "DWD Precipitation": test_dwd_precipitation_extractors(),
    }
    
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    for name, success in results.items():
        status = "✓ PASSED" if success else "✗ FAILED"
        print(f"{name:20} {status}")
    
    all_passed = all(results.values())
    print("\n" + "="*60)
    if all_passed:
        print("✓ ALL TESTS PASSED - UTC handling is working correctly!")
    else:
        print("✗ SOME TESTS FAILED - Please check the errors above")
    print("="*60)
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    exit(main())
