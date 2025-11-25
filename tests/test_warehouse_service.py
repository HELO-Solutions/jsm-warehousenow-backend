import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import httpx
import requests

from warehouse.warehouse_service import (
    fetch_warehouses_from_airtable,
    find_nearby_warehouses,
    _tier_rank,
    find_missing_fields
)

class TestWarehouseService:
    """Test cases for warehouse service functions"""

    @pytest.mark.asyncio
    async def test_fetch_warehouses_from_airtable_success(self):
        """Test successful warehouse fetching from Airtable"""
        mock_response = {
            "records": [
                {
                    "id": "rec123",
                    "fields": {"Name": "Test Warehouse", "City": "Test City"}
                }
            ]
        }
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_client.return_value.__aenter__.return_value = mock_instance
            
            # Create a mock response object
            mock_response_obj = MagicMock()
            mock_response_obj.json.return_value = mock_response
            mock_response_obj.raise_for_status = MagicMock()
            
            mock_instance.get = AsyncMock(return_value=mock_response_obj)
            
            result = await fetch_warehouses_from_airtable()
            
            assert len(result) == 1
            assert result[0]["id"] == "rec123"
            assert result[0]["fields"]["Name"] == "Test Warehouse"

    @pytest.mark.asyncio
    async def test_fetch_warehouses_from_airtable_error(self):
        """Test warehouse fetching with HTTP error"""
        with patch('httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_client.return_value.__aenter__.return_value = mock_instance
            mock_instance.get.side_effect = httpx.HTTPError("Test error")
            
            with pytest.raises(httpx.HTTPError):
                await fetch_warehouses_from_airtable()

    def test_tier_rank(self):
        """Test tier ranking function"""
        assert _tier_rank("Gold") == 0
        assert _tier_rank("Silver") == 1
        assert _tier_rank("Bronze") == 2
        assert _tier_rank("Unknown") == 99
        assert _tier_rank("") == 99
        assert _tier_rank(None) == 99

    def test_find_missing_fields(self):
        """Test missing fields detection"""
        complete_fields = {
            "City": "Test City",
            "State": "CA",
            "Zip": 90210,
            "Status": "Active"
        }
        
        incomplete_fields = {
            "City": "Test City",
            "State": "",
            "Zip": None,
            "Status": "Active"
        }
        
        # Test complete fields
        missing = find_missing_fields(complete_fields)
        assert len(missing) > 0  # Some fields will always be missing from FilterWarehouseData
        
        # Test incomplete fields
        missing = find_missing_fields(incomplete_fields)
        assert "State" in missing or "Zip" in missing

    @pytest.mark.asyncio
    async def test_find_nearby_warehouses_success(self):
        """Test successful nearby warehouse search"""
        mock_warehouses = [
            {
                "id": "rec123",
                "fields": {
                    "Name": "Test Warehouse",
                    "Zip": 90210,
                    "Tier": "Gold"
                }
            }
        ]
        
        with patch('warehouse.warehouse_service.fetch_warehouses_from_airtable', new_callable=AsyncMock) as mock_fetch, \
             patch('services.geolocation.geolocation_service.get_coordinates_mapbox') as mock_mapbox, \
             patch('services.geolocation.geolocation_service.get_coordinates_google') as mock_google, \
             patch('services.geolocation.geolocation_service.haversine') as mock_haversine, \
             patch('services.geolocation.geolocation_service.get_driving_distance_and_time_google', new_callable=AsyncMock) as mock_driving, \
             patch('warehouse.warehouse_service.analyze_warehouse_with_gemini', new_callable=AsyncMock) as mock_ai:
            
            mock_fetch.return_value = mock_warehouses
            mock_mapbox.return_value = (34.0522, -118.2437)  # LA coordinates
            mock_google.return_value = (34.0522, -118.2437)
            mock_haversine.return_value = 25.0  # Within radius
            mock_driving.return_value = {"distance_miles": 25.0, "duration_minutes": 30.0}
            mock_ai.return_value = "Test AI analysis"
            
            result = await find_nearby_warehouses("90210", 50.0)
            
            assert result["origin_zip"] == "90210"
            assert len(result["warehouses"]) == 1
            assert result["ai_analysis"] == "Test AI analysis"

    @pytest.mark.asyncio
    async def test_find_nearby_warehouses_invalid_zip(self):
        """Test nearby warehouse search with invalid ZIP code"""
        with patch('services.geolocation.geolocation_service.get_coordinates_mapbox') as mock_mapbox:
            mock_mapbox.return_value = None  # Invalid ZIP
            
            result = await find_nearby_warehouses("invalid", 50.0)
            
            assert "error" in result
            assert result["error"] == "Invalid ZIP code"
