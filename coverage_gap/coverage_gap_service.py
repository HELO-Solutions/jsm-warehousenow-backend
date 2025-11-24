"""
Coverage gap analysis service.
Provides comprehensive coverage gap analysis and AI-powered recommendations.
"""

import os
import json
from typing import List, Optional, Dict, Any, AsyncGenerator
from datetime import datetime, timezone
import math
import httpx
from dotenv import load_dotenv

from warehouse.warehouse_service import _cache, fetch_warehouses_from_airtable
from warehouse.models import (
    CoverageGapFilters,
    CoverageAnalysisResponse,
    AIAnalysisData,
    StaticWarehouseData,
    CoverageAnalysis,
    MockWarehouse
)
from services.gemini_services.coverage_gap_analysis import analyze_coverage_gaps_with_ai
from services.geolocation.geolocation_service import haversine

load_dotenv()
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("BASE_ID")
ODER_TABLE_NAME = "Requests"


async def get_total_requests_count() -> int:
    """Get total count of requests from the Requests table."""
    # Check cache first
    cached = _cache.get("requests:total_count")
    if cached is not None:
        return cached
    
    try:
        url = f"https://api.airtable.com/v0/{BASE_ID}/{ODER_TABLE_NAME}"
        headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
        params = {}
        
        total_count = 0
        async with httpx.AsyncClient() as client:
            offset = None
            while True:
                if offset:
                    params["offset"] = offset
                resp = await client.get(url, headers=headers, params=params)
                resp.raise_for_status()
                data = resp.json()
                total_count += len(data.get("records", []))
                offset = data.get("offset")
                if not offset:
                    break
            
            # Cache for 1 hour
            _cache.set("requests:total_count", total_count, ttl=3600)
            return total_count
    except Exception as e:
        print(f"Error getting total requests count: {e}")
        return 0


async def get_average_monthly_requests() -> int:
    """Calculate average number of requests per day for this month.
    
    Returns the average of requests created this month divided by the number of days elapsed,
    ALWAYS rounded UP to the nearest integer to ensure we never underestimate capacity needs.
    Example: If we're on day 10 of the month and have 50 requests, average = 50/10 = 5 requests/day.
    Example: If average is 13.5, it returns 14 (rounded up).
    """
    # Check cache first
    cached = _cache.get("requests:average_monthly")
    if cached is not None:
        return cached
    
    try:
        url = f"https://api.airtable.com/v0/{BASE_ID}/{ODER_TABLE_NAME}"
        headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
        params = {}
        
        # Get current month start
        now = datetime.now(timezone.utc)
        month_start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
        
        # Count requests created this month
        monthly_count = 0
        async with httpx.AsyncClient() as client:
            offset = None
            while True:
                if offset:
                    params["offset"] = offset
                resp = await client.get(url, headers=headers, params=params)
                resp.raise_for_status()
                data = resp.json()
                
                records = data.get("records", [])
                for record in records:
                    created_time_str = record.get("createdTime")
                    if created_time_str:
                        try:
                            # Parse the createdTime (ISO format: "2024-01-15T10:30:00.000Z")
                            created_time = datetime.fromisoformat(created_time_str.replace('Z', '+00:00'))
                            if created_time >= month_start:
                                monthly_count += 1
                        except (ValueError, AttributeError):
                            # Skip invalid date formats
                            continue
                
                offset = data.get("offset")
                if not offset:
                    break
        
        # Calculate average: monthly requests / days elapsed in current month
        days_elapsed = now.day
        average = monthly_count / days_elapsed if days_elapsed > 0 else float(monthly_count)
        
        # ALWAYS round UP to ensure we don't underestimate capacity needs
        result = math.ceil(average)
        
        # Cache for 1 hour (resets daily since it depends on days_elapsed)
        _cache.set("requests:average_monthly", result, ttl=3600)
        return result
    except Exception as e:
        print(f"Error calculating average monthly requests: {e}")
        return 0


def transform_warehouse_to_static_data(warehouse_record: dict, request_count: int = 0) -> StaticWarehouseData:
    """Transform Airtable warehouse record to StaticWarehouseData format."""
    fields = warehouse_record.get("fields", {})
    
    # Extract coordinates
    lat = fields.get("Latitude", 0.0)
    lng = fields.get("Longitude", 0.0)
    
    # Convert to float if they're strings
    if isinstance(lat, str):
        try:
            lat = float(lat)
        except (ValueError, TypeError):
            lat = 0.0
    if isinstance(lng, str):
        try:
            lng = float(lng)
        except (ValueError, TypeError):
            lng = 0.0
    
    # Handle list fields - convert to comma-separated strings
    def format_list_field(field_value):
        if isinstance(field_value, list):
            return ", ".join(str(item) for item in field_value)
        return str(field_value) if field_value is not None else ""
    
    # Handle error fields - convert to empty string if it's an error object
    def safe_string_field(field_value):
        if isinstance(field_value, dict) and 'error' in field_value:
            return ""  # Return empty string for error fields
        return str(field_value) if field_value is not None else ""
    
    return StaticWarehouseData(
        id=warehouse_record.get("id", ""),
        warehouse_id=safe_string_field(fields.get("WarehouseID", "")),
        name=safe_string_field(fields.get("Warehouse Name", "")),
        city=safe_string_field(fields.get("City", "")),
        state=safe_string_field(fields.get("State", "")),
        zipCode=safe_string_field(fields.get("ZIP", "")).strip(),
        status=format_list_field(fields.get("Status", "")),
        tier=safe_string_field(fields.get("Tier", "")),
        lat=lat,
        lng=lng,
        hazmat=safe_string_field(fields.get("Hazmat", "")),
        disposal=safe_string_field(fields.get("Disposal", "")),
        warehouseTempControlled=format_list_field(fields.get("Warehouse Temp Controlled", "")),
        foodGrade=safe_string_field(fields.get("Food Grade", "")),
        paperClamps=format_list_field(fields.get("Paper Clamps", "")),
        parkingSpots=format_list_field(fields.get("Parking Spots", "")),
        reqCount=request_count
    )


async def get_warehouse_request_counts() -> Dict[str, int]:
    """Get request counts per warehouse from the Requests table."""
    # Check cache first
    cached = _cache.get("requests:warehouse_counts")
    if cached is not None:
        return cached
    
    try:
        url = f"https://api.airtable.com/v0/{BASE_ID}/{ODER_TABLE_NAME}"
        headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
        params = {}
        
        warehouse_counts = {}
        async with httpx.AsyncClient() as client:
            offset = None
            while True:
                if offset:
                    params["offset"] = offset
                resp = await client.get(url, headers=headers, params=params)
                resp.raise_for_status()
                data = resp.json()
                
                records = data.get("records", [])
                for record in records:
                    fields = record.get("fields", {})
                    warehouse_field = fields.get("Warehouse", [])
                    
                    # Count requests per warehouse
                    for warehouse_id in warehouse_field:
                        if warehouse_id in warehouse_counts:
                            warehouse_counts[warehouse_id] += 1
                        else:
                            warehouse_counts[warehouse_id] = 1
                
                offset = data.get("offset")
                if not offset:
                    break
        
        # Cache for 1 hour
        _cache.set("requests:warehouse_counts", warehouse_counts, ttl=3600)
        return warehouse_counts
    except Exception as e:
        print(f"Error getting warehouse request counts: {e}")
        return {}


def apply_warehouse_filters(warehouses: List['StaticWarehouseData'], filters: 'CoverageGapFilters') -> List['StaticWarehouseData']:
    """Apply filters to warehouse list."""
    if not filters:
        return warehouses
    
    filtered = []
    
    for wh in warehouses:  # fixed indentation here
        # Tier filter
        if filters.tier:
            # Check if warehouse matches any of the requested tiers
            warehouse_tier = wh.tier or ""  # Handle None/empty
            warehouse_tier_stripped = warehouse_tier.strip() if isinstance(warehouse_tier, str) else ""
            
            # Determine if warehouse matches filter
            matches_filter = False
            
            for filter_tier in filters.tier:
                filter_tier_upper = filter_tier.strip().upper()
                
                # Gold filter includes both "Gold" and "Potential Gold"
                if filter_tier_upper == "GOLD":
                    if warehouse_tier_stripped.upper() in ["GOLD", "POTENTIAL GOLD"]:
                        matches_filter = True
                        break
                # Un-tiered filter: empty, null, or not in standard tiers
                elif filter_tier_upper == "UN-TIERED" or filter_tier_upper == "UNTIERED":
                    standard_tiers = ["GOLD", "POTENTIAL GOLD", "SILVER", "BRONZE"]
                    if not warehouse_tier_stripped or warehouse_tier_stripped.upper() not in standard_tiers:
                        matches_filter = True
                        break
                # Exact match for other tiers (Silver, Bronze, etc.)
                else:
                    if warehouse_tier_stripped.upper() == filter_tier_upper:
                        matches_filter = True
                        break
            
            if not matches_filter:
                continue
        
        # State filter
        if filters.state and wh.state != filters.state:
            continue
        
        # City filter
        if filters.city and wh.city != filters.city:
            continue
        
        # Hazmat filter (needs to check if value is in list)
        if filters.hazmat:
            if wh.hazmat not in filters.hazmat:
                continue
        
        # Disposal filter
        if filters.disposal:
            if wh.disposal not in filters.disposal:
                continue
        
        # Warehouse Temp Controlled filter
        if filters.warehouseTempControlled:
            if not wh.warehouseTempControlled:
                continue  # No warehouse temp controlled value, skip
            wh_temp_list = [item.strip().upper() for item in wh.warehouseTempControlled.split(',') if item.strip()]
            filter_temp_upper = [t.strip().upper() for t in filters.warehouseTempControlled]
            if not any(temp in wh_temp_list for temp in filter_temp_upper):
                continue
        
        # Food Grade filter
        if filters.foodGrade:
            if not wh.foodGrade:
                continue  # No food grade value, skip
            food_grade_value = wh.foodGrade.strip().upper()
            if not any(fg.strip().upper() == food_grade_value for fg in filters.foodGrade):
                continue
        
        # Paper Clamps filter
        if filters.paperClamps:
            if not wh.paperClamps:
                continue  # No paper clamps value, skip
            clamps_list = [item.strip().upper() for item in wh.paperClamps.split(',') if item.strip()]
            filter_clamps_upper = [c.strip().upper() for c in filters.paperClamps]
            if not any(clamp in clamps_list for clamp in filter_clamps_upper):
                continue
        
        # Parking Spots filter
        if filters.parkingSpots:
            if not wh.parkingSpots:
                continue  # No parking spots value, skip
            parking_list = [item.strip().upper() for item in wh.parkingSpots.split(',') if item.strip()]
            filter_parking_upper = [p.strip().upper() for p in filters.parkingSpots]
            if not any(spot in parking_list for spot in filter_parking_upper):
                continue
        
        filtered.append(wh)
    
    return filtered


def load_us_cities() -> Dict[str, Dict]:
    """Load all US cities from us_cities.json and return as dict keyed by city,state"""
    import json
    
    try:
        with open('data/us_cities.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        cities_dict = {}
        for city_data in data.get('cities', []):
            city_key = f"{city_data['city']},{city_data['state']}"
            cities_dict[city_key] = city_data
        
        print(f"Loaded {len(cities_dict)} US cities from us_cities.json")
        return cities_dict
    except FileNotFoundError:
        print("Warning: us_cities.json not found. Run generate_us_cities.py first.")
        return {}
    except Exception as e:
        print(f"Error loading US cities: {e}")
        return {}


async def get_coverage_gap_analysis_stream(
    filters: Optional[CoverageGapFilters] = None, 
    radius_miles: Optional[float] = None
) -> AsyncGenerator[str, None]:
    """Get comprehensive coverage gap analysis with streaming progress updates via SSE."""
    
    def format_log(message: str, progress: Optional[float] = None) -> str:
        """Helper to format SSE log messages"""
        log_data = {"type": "log", "message": message}
        if progress is not None:
            log_data["progress"] = progress
        return f"data: {json.dumps(log_data)}\n\n"
    
    def format_data(data: CoverageAnalysisResponse) -> str:
        """Helper to format final result"""
        # Use model_dump with mode='json' to ensure proper serialization
        return f"data: {json.dumps({'type': 'data', 'data': data.model_dump(mode='json')})}\n\n"
    
    def format_error(error: str) -> str:
        """Helper to format error message"""
        return f"data: {json.dumps({'type': 'error', 'message': error})}\n\n"
    
    try:
        # Create cache key based on filters and radius
        if filters:
            filter_dict = filters.model_dump(exclude_none=True, exclude_unset=True)
            filter_key = json.dumps(filter_dict, sort_keys=True)
        else:
            filter_key = "no_filters"
        
        radius_key = f"_radius_{radius_miles}" if radius_miles else "_no_radius"
        cache_key = f"coverage_gap:{filter_key}{radius_key}"
        
        # Check cache first
        cached = _cache.get(cache_key)
        if cached:
            print("=== COVERAGE GAP ANALYSIS (CACHED) ===")
            print(f"DEBUG: Cache key: {cache_key}")
            yield format_log("Using cached results")
            yield format_data(cached)
            return
        
        print("=== COVERAGE GAP ANALYSIS STARTED ===")
        print(f"DEBUG: Cache key: {cache_key}")
        
        # Step 1: Fetch warehouses
        yield format_log("Fetching warehouses from Airtable...", 5)
        warehouses_data = await fetch_warehouses_from_airtable()
        print(f"Fetched {len(warehouses_data)} warehouses from Airtable")
        yield format_log(f"Fetched {len(warehouses_data)} warehouses from Airtable", 10)
        
        # Step 2: Get total requests count
        yield format_log("Calculating total request count...", 12)
        total_requests = await get_total_requests_count()
        print(f"Total requests: {total_requests}")
        yield format_log(f"Total requests: {total_requests}", 20)
        
        # Step 3: Get warehouse request counts
        yield format_log("Fetching request counts per warehouse...", 22)
        warehouse_request_counts = await get_warehouse_request_counts()
        print(f"Fetched request counts for {len(warehouse_request_counts)} warehouses")
        yield format_log(f"Fetched request counts for {len(warehouse_request_counts)} warehouses", 30)
        
        # Step 4: Transform warehouses
        yield format_log("Transforming warehouse data...", 32)
        static_warehouses = []
        for idx, warehouse_record in enumerate(warehouses_data):
            warehouse_id = warehouse_record.get("id", "")
            request_count = warehouse_request_counts.get(warehouse_id, 0)
            static_warehouse = transform_warehouse_to_static_data(warehouse_record, request_count)
            static_warehouses.append(static_warehouse)
            if (idx + 1) % 500 == 0:
                yield format_log(f"Transformed {idx + 1}/{len(warehouses_data)} warehouses...", 32 + int((idx + 1) / len(warehouses_data) * 6))
        print(f"Transformed {len(static_warehouses)} warehouses")
        yield format_log(f"Transformed {len(static_warehouses)} warehouses", 40)
        
        # Step 5: Apply filters if provided
        if filters:
            print(f"Applying filters: {filters}")
            yield format_log(f"Applying filters: {filters}", 42)
            static_warehouses = apply_warehouse_filters(static_warehouses, filters)
            print(f"After filtering: {len(static_warehouses)} warehouses")
            yield format_log(f"After filtering: {len(static_warehouses)} warehouses remain", 45)
        
        # Step 6: Get average monthly requests
        yield format_log("Calculating average monthly requests...", 47)
        average_monthly_requests = await get_average_monthly_requests()
        print(f"Average monthly requests: {average_monthly_requests}")
        yield format_log(f"Average monthly requests: {average_monthly_requests}", 50)
        
        # Step 7: Load all US cities
        print("Loading all US cities...")
        yield format_log("Loading all US cities from database...", 52)
        us_cities = load_us_cities()
        print(f"Loaded {len(us_cities)} US cities from us_cities.json")
        yield format_log(f"Loaded {len(us_cities)} US cities from database", 55)
        
        # Step 8: Group warehouses by city
        print("Grouping warehouses by city")
        yield format_log("Grouping warehouses by city...", 57)
        warehouse_city_data = {}
        for warehouse in static_warehouses:
            city = warehouse.city.strip() if warehouse.city else ""
            state = warehouse.state.strip() if warehouse.state else ""
            if not city or not state:
                continue
            
            city_key = f"{city},{state}"
            
            if city_key not in warehouse_city_data:
                warehouse_city_data[city_key] = {
                    "warehouses": [],
                    "totalRequests": 0
                }
            
            warehouse_city_data[city_key]["warehouses"].append(warehouse)
            warehouse_city_data[city_key]["totalRequests"] += warehouse.reqCount
        print(f"Grouped warehouses into {len(warehouse_city_data)} cities")
        yield format_log(f"Grouped warehouses into {len(warehouse_city_data)} cities", 60)
        
        # Step 9: Radius expansion if provided
        if radius_miles and radius_miles > 0:
            print(f"Expanding all cities with radius: {radius_miles} miles")
            yield format_log(f"Expanding coverage with {radius_miles} mile radius...", 62)
            
            valid_warehouses = [wh for wh in static_warehouses if wh.lat != 0 and wh.lng != 0]
            print(f"  Processing {len(valid_warehouses)} warehouses with valid coordinates")
            print(f"  Checking against {len(us_cities)} US cities")
            yield format_log(f"Processing {len(valid_warehouses)} warehouses against {len(us_cities)} cities...", 65)
            
            # First, expand existing warehouse city groups
            yield format_log("Expanding existing warehouse cities...", 67)
            for city_key, data in warehouse_city_data.items():
                # Get city coordinates from US cities data or calculate from warehouses
                city_info = us_cities.get(city_key, {})
                if city_info and city_info.get('latitude') and city_info.get('longitude'):
                    center_lat = city_info['latitude']
                    center_lng = city_info['longitude']
                else:
                    # Calculate from warehouses
                    valid_coords = [(wh.lat, wh.lng) for wh in data["warehouses"] 
                                  if wh.lat != 0 and wh.lng != 0]
                    if not valid_coords:
                        continue
                    center_lat = sum(coord[0] for coord in valid_coords) / len(valid_coords)
                    center_lng = sum(coord[1] for coord in valid_coords) / len(valid_coords)
                
                existing_warehouse_ids = {wh.id for wh in data["warehouses"]}
                
                for warehouse in valid_warehouses:
                    if warehouse.id in existing_warehouse_ids:
                        continue
                    
                    distance = haversine(center_lat, center_lng, warehouse.lat, warehouse.lng)
                    if distance <= radius_miles:
                        data["warehouses"].append(warehouse)
                        data["totalRequests"] += warehouse.reqCount
                        existing_warehouse_ids.add(warehouse.id)
            
            # Second, check ALL US cities (including those without warehouses) for nearby warehouses
            processed = 0
            total_cities = len(us_cities)
            yield format_log(f"Checking all {total_cities} US cities for nearby warehouses...", 70)
            
            for city_key, city_info in us_cities.items():
                processed += 1
                if processed % 5000 == 0:
                    progress = 70 + int((processed / total_cities) * 20)  # 70-90% range
                    print(f"  Processing city {processed}/{total_cities}...")
                    yield format_log(f"Processing city {processed}/{total_cities}...", progress)
                
                # Skip if city already has warehouses (already processed above)
                if city_key in warehouse_city_data:
                    continue
                
                # Skip if city has no valid coordinates
                if not city_info.get('latitude') or not city_info.get('longitude'):
                    continue
                
                center_lat = city_info['latitude']
                center_lng = city_info['longitude']
                
                # Check if any warehouses fall within radius of this city
                nearby_warehouses_for_city = []
                for warehouse in valid_warehouses:
                    distance = haversine(center_lat, center_lng, warehouse.lat, warehouse.lng)
                    if distance <= radius_miles:
                        nearby_warehouses_for_city.append(warehouse)
                
                # If warehouses found within radius, add them to warehouse_city_data
                if nearby_warehouses_for_city:
                    warehouse_city_data[city_key] = {
                        "warehouses": nearby_warehouses_for_city,
                        "totalRequests": sum(wh.reqCount for wh in nearby_warehouses_for_city)
                    }
            
            print(f"  Radius expansion completed. Cities with warehouses after expansion: {len(warehouse_city_data)}")
            yield format_log(f"Radius expansion completed. {len(warehouse_city_data)} cities now have warehouses", 90)
        
        # Step 10: Create coverage analysis for ALL US cities
        print(f"Creating coverage analysis for all {len(us_cities)} US cities...")
        yield format_log(f"Creating coverage analysis for all {len(us_cities)} US cities...", 92)
        coverage_analysis = []
        
        processed_cities = 0
        for city_key, city_info in us_cities.items():
            processed_cities += 1
            if processed_cities % 5000 == 0:
                progress = 92 + int((processed_cities / len(us_cities)) * 6)  # 92-98% range
                yield format_log(f"Analyzing city {processed_cities}/{len(us_cities)}...", progress)
            
            # Get warehouse data for this city if available
            warehouse_data = warehouse_city_data.get(city_key, {})
            warehouses_in_city = warehouse_data.get("warehouses", [])
            total_requests_in_city = warehouse_data.get("totalRequests", 0)
            
            # Count warehouses by tier
            gold_count = sum(1 for w in warehouses_in_city if w.tier in ["Gold", "Potential Gold"])
            silver_count = sum(1 for w in warehouses_in_city if w.tier == "Silver")
            bronze_count = sum(1 for w in warehouses_in_city if w.tier == "Bronze")
            standard_tiers = ["Gold", "Potential Gold", "Silver", "Bronze"]
            un_tiered_count = sum(1 for w in warehouses_in_city if not w.tier or (isinstance(w.tier, str) and w.tier.strip() == "") or (w.tier not in standard_tiers))
            
            # Create nearby warehouses list (top 3)
            nearby_warehouses = []
            for wh in warehouses_in_city[:3]:
                distance = 0.0
                if len(warehouses_in_city) > 1:
                    other_warehouses = [w for w in warehouses_in_city if w.id != wh.id]
                    if other_warehouses:
                        distances = []
                        for other_wh in other_warehouses:
                            if wh.lat != 0 and wh.lng != 0 and other_wh.lat != 0 and other_wh.lng != 0:
                                dist = haversine(wh.lat, wh.lng, other_wh.lat, other_wh.lng)
                                distances.append(dist)
                        distance = sum(distances) / len(distances) if distances else 0.0
                
                nearby_warehouses.append(MockWarehouse(
                    id=wh.id,
                    name=wh.name,
                    tier=wh.tier,
                    distance=distance
                ))
            
            warehouse_count = len(warehouses_in_city)
            avg_requests_per_warehouse = total_requests_in_city / warehouse_count if warehouse_count > 0 else 0
            
            # Determine expansion opportunity
            if avg_requests_per_warehouse > 25:
                expansion_opportunity = "High"
            elif warehouse_count == 1 and total_requests_in_city > 10:
                expansion_opportunity = "High"
            elif warehouse_count == 1 and total_requests_in_city > 3:
                expansion_opportunity = "Moderate"
            elif warehouse_count < 3 and total_requests_in_city > 15:
                expansion_opportunity = "Moderate"
            elif avg_requests_per_warehouse > 15:
                expansion_opportunity = "Moderate"
            elif total_requests_in_city == 0:
                expansion_opportunity = "None"
            else:
                expansion_opportunity = "None"
            
            # Calculate warehouses per 100 sq miles
            estimated_city_area_sq_miles = max(warehouse_count * 25, 50)
            warehouses_per_100_sq_miles = (warehouse_count / estimated_city_area_sq_miles) * 100 if estimated_city_area_sq_miles > 0 else 0
            
            # Determine coverage gap
            has_coverage_gap = warehouse_count < 2 or avg_requests_per_warehouse > 20
            
            coverage_analysis.append(CoverageAnalysis(
                city=city_info["city"],
                state=city_info["state"],
                latitude=city_info["latitude"],
                longitude=city_info["longitude"],
                zipcodes=city_info["zipcodes"],
                nearbyWarehouses=nearby_warehouses,
                warehouseCount=warehouse_count,
                hasCoverageGap=has_coverage_gap,
                expansionOpportunity=expansion_opportunity,
                goldWarehouseCount=gold_count,
                silverWarehouseCount=silver_count,
                bronzeWarehouseCount=bronze_count,
                unTieredWarehouseCount=un_tiered_count,
                warehousesPer100SqMiles=warehouses_per_100_sq_miles,
                reqCount=total_requests_in_city
            ))
        
        # Step 11: Build final result
        yield format_log("Finalizing results...", 98)
        result = CoverageAnalysisResponse(
            warehouses=static_warehouses,
            coverageAnalysis=coverage_analysis,
            average_number_of_requests=average_monthly_requests,
            totalWarehouses=len(static_warehouses),
            totalRequests=total_requests,
            analysisRadius=radius_miles if radius_miles else 50
        )
        
        # Cache the result for 30 minutes
        _cache.set(cache_key, result, ttl=1800)
        
        yield format_log("Analysis complete!", 100)
        yield format_data(result)
        
    except Exception as e:
        error_msg = f"Coverage gap analysis failed: {str(e)}"
        print(f"Error in coverage gap analysis: {error_msg}")
        yield format_error(error_msg)
        raise


async def get_coverage_gap_analysis(filters: Optional[CoverageGapFilters] = None, radius_miles: Optional[float] = None) -> CoverageAnalysisResponse:
    """Get comprehensive coverage gap analysis for all US cities with warehouse data where available."""
    
    # Create cache key based on filters and radius
    import json
    if filters:
        filter_dict = filters.model_dump(exclude_none=True, exclude_unset=True)
        filter_key = json.dumps(filter_dict, sort_keys=True)
    else:
        filter_key = "no_filters"
    
    radius_key = f"_radius_{radius_miles}" if radius_miles else "_no_radius"
    cache_key = f"coverage_gap:{filter_key}{radius_key}"
    
    # Check cache first
    cached = _cache.get(cache_key)
    if cached:
        print("=== COVERAGE GAP ANALYSIS (CACHED) ===")
        print(f"DEBUG: Cache key: {cache_key}")
        return cached
    
    print("=== COVERAGE GAP ANALYSIS STARTED ===")
    print(f"DEBUG: Cache key: {cache_key}")
    
    # Get all warehouses
    warehouses_data = await fetch_warehouses_from_airtable()
    
    # Get total requests count
    total_requests = await get_total_requests_count()
    
    # Get warehouse request counts
    warehouse_request_counts = await get_warehouse_request_counts()
    
    # Transform warehouses to StaticWarehouseData format
    static_warehouses = []
    for warehouse_record in warehouses_data:
        warehouse_id = warehouse_record.get("id", "")
        request_count = warehouse_request_counts.get(warehouse_id, 0)
        static_warehouse = transform_warehouse_to_static_data(warehouse_record, request_count)
        static_warehouses.append(static_warehouse)
    
    # Apply filters if provided
    if filters:
        print(f"Applying filters: {filters}")
        static_warehouses = apply_warehouse_filters(static_warehouses, filters)
        print(f"After filtering: {len(static_warehouses)} warehouses")
    
    # Get average monthly requests
    average_monthly_requests = await get_average_monthly_requests()
    print(f"Average monthly requests: {average_monthly_requests}")
    
    # Load all US cities
    print("Loading all US cities...")
    us_cities = load_us_cities()
    
    # Group warehouses by city
    print("Grouping warehouses by city")
    warehouse_city_data = {}
    for warehouse in static_warehouses:
        city = warehouse.city.strip() if warehouse.city else ""
        state = warehouse.state.strip() if warehouse.state else ""
        if not city or not state:
            continue
        
        city_key = f"{city},{state}"
        
        if city_key not in warehouse_city_data:
            warehouse_city_data[city_key] = {
                "warehouses": [],
                "totalRequests": 0
            }
        
        warehouse_city_data[city_key]["warehouses"].append(warehouse)
        warehouse_city_data[city_key]["totalRequests"] += warehouse.reqCount
    
    # If radius is provided, expand ALL cities (including those without warehouses) to include nearby warehouses
    if radius_miles and radius_miles > 0:
        print(f"Expanding all cities with radius: {radius_miles} miles")
        
        valid_warehouses = [wh for wh in static_warehouses if wh.lat != 0 and wh.lng != 0]
        print(f"  Processing {len(valid_warehouses)} warehouses with valid coordinates")
        print(f"  Checking against {len(us_cities)} US cities")
        
        # First, expand existing warehouse city groups
        for city_key, data in warehouse_city_data.items():
            # Get city coordinates from US cities data or calculate from warehouses
            city_info = us_cities.get(city_key, {})
            if city_info and city_info.get('latitude') and city_info.get('longitude'):
                center_lat = city_info['latitude']
                center_lng = city_info['longitude']
            else:
                # Calculate from warehouses
                valid_coords = [(wh.lat, wh.lng) for wh in data["warehouses"] 
                              if wh.lat != 0 and wh.lng != 0]
                if not valid_coords:
                    continue
                center_lat = sum(coord[0] for coord in valid_coords) / len(valid_coords)
                center_lng = sum(coord[1] for coord in valid_coords) / len(valid_coords)
            
            existing_warehouse_ids = {wh.id for wh in data["warehouses"]}
            
            for warehouse in valid_warehouses:
                if warehouse.id in existing_warehouse_ids:
                    continue
                
                distance = haversine(center_lat, center_lng, warehouse.lat, warehouse.lng)
                if distance <= radius_miles:
                    data["warehouses"].append(warehouse)
                    data["totalRequests"] += warehouse.reqCount
                    existing_warehouse_ids.add(warehouse.id)
        
        # Second, check ALL US cities (including those without warehouses) for nearby warehouses
        processed = 0
        for city_key, city_info in us_cities.items():
            processed += 1
            if processed % 5000 == 0:
                print(f"  Processing city {processed}/{len(us_cities)}...")
            
            # Skip if city already has warehouses (already processed above)
            if city_key in warehouse_city_data:
                continue
            
            # Skip if city has no valid coordinates
            if not city_info.get('latitude') or not city_info.get('longitude'):
                continue
            
            center_lat = city_info['latitude']
            center_lng = city_info['longitude']
            
            # Check if any warehouses fall within radius of this city
            nearby_warehouses_for_city = []
            for warehouse in valid_warehouses:
                distance = haversine(center_lat, center_lng, warehouse.lat, warehouse.lng)
                if distance <= radius_miles:
                    nearby_warehouses_for_city.append(warehouse)
            
            # If warehouses found within radius, add them to warehouse_city_data
            if nearby_warehouses_for_city:
                warehouse_city_data[city_key] = {
                    "warehouses": nearby_warehouses_for_city,
                    "totalRequests": sum(wh.reqCount for wh in nearby_warehouses_for_city)
                }
        
        print(f"  Radius expansion completed. Cities with warehouses after expansion: {len(warehouse_city_data)}")
    
    # Create coverage analysis for ALL US cities
    print(f"Creating coverage analysis for all {len(us_cities)} US cities...")
    coverage_analysis = []
    
    for city_key, city_info in us_cities.items():
        # Get warehouse data for this city if available
        warehouse_data = warehouse_city_data.get(city_key, {})
        warehouses_in_city = warehouse_data.get("warehouses", [])
        total_requests_in_city = warehouse_data.get("totalRequests", 0)
        
        # Count warehouses by tier
        gold_count = sum(1 for w in warehouses_in_city if w.tier in ["Gold", "Potential Gold"])
        silver_count = sum(1 for w in warehouses_in_city if w.tier == "Silver")
        bronze_count = sum(1 for w in warehouses_in_city if w.tier == "Bronze")
        standard_tiers = ["Gold", "Potential Gold", "Silver", "Bronze"]
        un_tiered_count = sum(1 for w in warehouses_in_city if not w.tier or (isinstance(w.tier, str) and w.tier.strip() == "") or (w.tier not in standard_tiers))
        
        # Create nearby warehouses list (top 3)
        nearby_warehouses = []
        for wh in warehouses_in_city[:3]:
            distance = 0.0
            if len(warehouses_in_city) > 1:
                other_warehouses = [w for w in warehouses_in_city if w.id != wh.id]
                if other_warehouses:
                    distances = []
                    for other_wh in other_warehouses:
                        if wh.lat != 0 and wh.lng != 0 and other_wh.lat != 0 and other_wh.lng != 0:
                            dist = haversine(wh.lat, wh.lng, other_wh.lat, other_wh.lng)
                            distances.append(dist)
                    distance = sum(distances) / len(distances) if distances else 0.0
            
            nearby_warehouses.append(MockWarehouse(
                id=wh.id,
                name=wh.name,
                tier=wh.tier,
                distance=distance
            ))
        
        warehouse_count = len(warehouses_in_city)
        avg_requests_per_warehouse = total_requests_in_city / warehouse_count if warehouse_count > 0 else 0
        
        # Determine expansion opportunity
        if avg_requests_per_warehouse > 25:
            expansion_opportunity = "High"
        elif warehouse_count == 1 and total_requests_in_city > 10:
            expansion_opportunity = "High"
        elif warehouse_count == 1 and total_requests_in_city > 3:
            expansion_opportunity = "Moderate"
        elif warehouse_count < 3 and total_requests_in_city > 15:
            expansion_opportunity = "Moderate"
        elif avg_requests_per_warehouse > 15:
            expansion_opportunity = "Moderate"
        elif total_requests_in_city == 0:
            expansion_opportunity = "None"
        else:
            expansion_opportunity = "None"
        
        # Calculate warehouses per 100 sq miles
        estimated_city_area_sq_miles = max(warehouse_count * 25, 50)
        warehouses_per_100_sq_miles = (warehouse_count / estimated_city_area_sq_miles) * 100 if estimated_city_area_sq_miles > 0 else 0
        
        # Determine coverage gap
        has_coverage_gap = warehouse_count < 2 or avg_requests_per_warehouse > 20
        
        coverage_analysis.append(CoverageAnalysis(
            city=city_info["city"],
            state=city_info["state"],
            latitude=city_info["latitude"],
            longitude=city_info["longitude"],
            zipcodes=city_info["zipcodes"],
            nearbyWarehouses=nearby_warehouses,
            warehouseCount=warehouse_count,
            hasCoverageGap=has_coverage_gap,
            expansionOpportunity=expansion_opportunity,
            goldWarehouseCount=gold_count,
            silverWarehouseCount=silver_count,
            bronzeWarehouseCount=bronze_count,
            unTieredWarehouseCount=un_tiered_count,
            warehousesPer100SqMiles=warehouses_per_100_sq_miles,
            reqCount=total_requests_in_city
        ))
    
    result = CoverageAnalysisResponse(
        warehouses=static_warehouses,
        coverageAnalysis=coverage_analysis,
        average_number_of_requests=average_monthly_requests,
        totalWarehouses=len(static_warehouses),
        totalRequests=total_requests,
        analysisRadius=radius_miles if radius_miles else 50  # Use provided radius or default to 50
    )
    
    # Cache the result for 30 minutes
    _cache.set(cache_key, result, ttl=1800)
    return result


async def get_ai_analysis_only(filters: Optional[CoverageGapFilters] = None, radius_miles: Optional[float] = None) -> AIAnalysisData:
    """Get only AI analysis for coverage gaps."""
    
    print("=== AI ANALYSIS STARTED ===")
    
    # Get all warehouses
    warehouses_data = await fetch_warehouses_from_airtable()
    
    # Get total requests count
    total_requests = await get_total_requests_count()
    
    # Get warehouse request counts
    warehouse_request_counts = await get_warehouse_request_counts()
    
    # Transform warehouses to StaticWarehouseData format
    static_warehouses = []
    for warehouse_record in warehouses_data:
        warehouse_id = warehouse_record.get("id", "")
        request_count = warehouse_request_counts.get(warehouse_id, 0)
        static_warehouse = transform_warehouse_to_static_data(warehouse_record, request_count)
        static_warehouses.append(static_warehouse)
    
    # Apply filters if provided
    if filters:
        print(f"Applying filters to AI analysis: {filters}")
        static_warehouses = apply_warehouse_filters(static_warehouses, filters)
        print(f"After filtering: {len(static_warehouses)} warehouses for AI analysis")
    
    # Group warehouses by city (NO radius expansion for AI analysis)
    print("Grouping warehouses by city for AI analysis (no radius expansion)")
    city_warehouses_dict = {}
    for warehouse in static_warehouses:
        city = warehouse.city.strip() if warehouse.city else ""
        state = warehouse.state.strip() if warehouse.state else ""
        if not city or not state:
            continue
        
        # Use city+state as composite key to handle cities with same name in different states
        city_key = f"{city},{state}"
        
        if city_key not in city_warehouses_dict:
            city_warehouses_dict[city_key] = {
                "city": warehouse.city,
                "state": warehouse.state,
                "warehouses": [],
                "totalRequests": 0
            }
        city_warehouses_dict[city_key]["warehouses"].append(warehouse)
        city_warehouses_dict[city_key]["totalRequests"] += warehouse.reqCount
    
    # Pass the grouped city data (NO radius expansion) directly to AI analysis
    print(f"Starting AI analysis for {len(city_warehouses_dict)} city groups (no radius expansion)")
    
    # Get total unique warehouses for AI prompt
    unique_warehouse_ids = set()
    for data in city_warehouses_dict.values():
        for warehouse in data["warehouses"]:
            unique_warehouse_ids.add(warehouse.id)
    
    ai_analysis = await analyze_coverage_gaps_with_ai(city_warehouses_dict, total_requests, len(unique_warehouse_ids))
    print(f"AI analysis completed: {len(ai_analysis.coverageGaps)} gaps, {len(ai_analysis.recommendations)} recommendations")
    
    return ai_analysis

