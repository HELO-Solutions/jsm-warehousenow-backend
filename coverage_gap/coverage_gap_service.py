"""
Coverage gap analysis service.
Provides comprehensive coverage gap analysis and AI-powered recommendations.
"""

import os
from typing import List, Optional, Dict, Any
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


async def get_coverage_gap_analysis(filters: Optional[CoverageGapFilters] = None, radius_miles: Optional[float] = None) -> CoverageAnalysisResponse:
    """Get comprehensive coverage gap analysis with warehouses (grouped by zipcode or radius)."""
    
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
        # Still check for 91761 in cached data for debugging
        if hasattr(cached, 'coverageAnalysis'):
            zipcodes_in_cache = [ca.zipCode for ca in cached.coverageAnalysis]
            if "91761" in zipcodes_in_cache:
                print(f"DEBUG: Zipcode 91761 found in CACHED response with {len([ca for ca in cached.coverageAnalysis if ca.zipCode == '91761'])} entries")
            else:
                print(f"DEBUG: Zipcode 91761 NOT found in CACHED response. Zipcodes in cache: {zipcodes_in_cache[:10]}...")
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
    
    # Create coverage analysis with REAL data
    coverage_analysis = []
    
    # Group warehouses by zipcode first, then expand with radius if provided
    print("Grouping warehouses by zipcode")
    zipcode_analysis = {}
    for warehouse in static_warehouses:
        zipcode = warehouse.zipCode.strip() if warehouse.zipCode else ""
        if not zipcode:
            continue  # Skip warehouses without zip codes
        
        if zipcode not in zipcode_analysis:
            zipcode_analysis[zipcode] = {
                "city": warehouse.city,
                "state": warehouse.state,
                "zipCode": warehouse.zipCode,
                "warehouses": [],
                "lat": warehouse.lat,
                "lng": warehouse.lng,
                "totalRequests": 0
            }
        zipcode_analysis[zipcode]["warehouses"].append(warehouse)
        zipcode_analysis[zipcode]["totalRequests"] += warehouse.reqCount
    
    # Debug: Check if 91761 exists after initial grouping
    if "91761" in zipcode_analysis:
        print(f"DEBUG: Zipcode 91761 found with {len(zipcode_analysis['91761']['warehouses'])} warehouses and {zipcode_analysis['91761']['totalRequests']} requests")
    else:
        print(f"DEBUG: Zipcode 91761 NOT found in zipcode_analysis after initial grouping")
        # Check if any warehouses have zipcode 91761
        warehouses_91761 = [wh for wh in static_warehouses if wh.zipCode == "91761"]
        print(f"DEBUG: Found {len(warehouses_91761)} warehouses with zipcode 91761 in static_warehouses")
    
    # If radius is provided, expand each zipcode group to include nearby warehouses
    if radius_miles and radius_miles > 0:
        print(f"Expanding zipcode groups with radius: {radius_miles} miles")
        
        # Pre-filter warehouses with valid coordinates to avoid checking invalid ones repeatedly
        valid_warehouses = [wh for wh in static_warehouses if wh.lat != 0 and wh.lng != 0]
        print(f"  Processing {len(valid_warehouses)} warehouses with valid coordinates")
        
        # Track which warehouses belong to which zipcodes to avoid duplicates
        warehouse_zipcode_map = {}
        for zipcode, data in zipcode_analysis.items():
            for wh in data["warehouses"]:
                if wh.id not in warehouse_zipcode_map:
                    warehouse_zipcode_map[wh.id] = set()
                warehouse_zipcode_map[wh.id].add(zipcode)
        
        # For each zipcode group, find warehouses within radius and add them
        zipcode_count = len(zipcode_analysis)
        processed = 0
        for zipcode, data in zipcode_analysis.items():
            processed += 1
            if processed % 50 == 0:
                print(f"  Processing zipcode {processed}/{zipcode_count}...")
            
            # Calculate center point of this zipcode group (average of warehouse locations)
            valid_coords = [(wh.lat, wh.lng) for wh in data["warehouses"] 
                          if wh.lat != 0 and wh.lng != 0]
            if not valid_coords:
                continue  # Skip if no valid coordinates
            
            center_lat = sum(coord[0] for coord in valid_coords) / len(valid_coords)
            center_lng = sum(coord[1] for coord in valid_coords) / len(valid_coords)
            
            # Track which warehouses are already in this zipcode group
            existing_warehouse_ids = {wh.id for wh in data["warehouses"]}
            
            # Find all warehouses within radius of this zipcode's center
            # Only check warehouses with valid coordinates
            added_count = 0
            for warehouse in valid_warehouses:
                # Skip if warehouse is already in this zipcode group
                if warehouse.id in existing_warehouse_ids:
                    continue
                
                # Calculate distance from zipcode center to warehouse
                distance = haversine(center_lat, center_lng, warehouse.lat, warehouse.lng)
                
                # If within radius, add to this zipcode group
                if distance <= radius_miles:
                    data["warehouses"].append(warehouse)
                    data["totalRequests"] += warehouse.reqCount
                    existing_warehouse_ids.add(warehouse.id)
                    added_count += 1
                    # Track this warehouse in the zipcode map
                    if warehouse.id not in warehouse_zipcode_map:
                        warehouse_zipcode_map[warehouse.id] = set()
                    warehouse_zipcode_map[warehouse.id].add(zipcode)
        
        print(f"  Radius expansion completed for {zipcode_count} zipcodes")
    
    # Debug: Log all zipcodes before creating coverage analysis
    print(f"DEBUG: Creating coverage analysis for {len(zipcode_analysis)} zipcodes")
    if "91761" in zipcode_analysis:
        print(f"DEBUG: Zipcode 91761 will be included in coverage analysis with {len(zipcode_analysis['91761']['warehouses'])} warehouses")
    
    # Create coverage analysis for each zipcode with REAL calculations
    for zipcode, data in zipcode_analysis.items():
        warehouses_in_zipcode = data["warehouses"]
        
        # Count warehouses by tier (REAL data)
        # Gold count includes both "Gold" and "Potential Gold" tiers
        gold_count = sum(1 for w in warehouses_in_zipcode if w.tier in ["Gold", "Potential Gold"])
        silver_count = sum(1 for w in warehouses_in_zipcode if w.tier == "Silver")
        bronze_count = sum(1 for w in warehouses_in_zipcode if w.tier == "Bronze")
        # Un-tiered warehouses: empty, null, or not in standard tiers
        standard_tiers = ["Gold", "Potential Gold", "Silver", "Bronze"]
        un_tiered_count = sum(1 for w in warehouses_in_zipcode if not w.tier or (isinstance(w.tier, str) and w.tier.strip() == "") or (w.tier not in standard_tiers))
        
        # Calculate REAL minimum distance between warehouses in the zipcode
        min_distance = 0.0  # Default to 0 for single warehouse
        if len(warehouses_in_zipcode) > 1:
            min_distance = float('inf')
            for i, wh1 in enumerate(warehouses_in_zipcode):
                for wh2 in warehouses_in_zipcode[i+1:]:
                    if wh1.lat != 0 and wh1.lng != 0 and wh2.lat != 0 and wh2.lng != 0:
                        distance = haversine(wh1.lat, wh1.lng, wh2.lat, wh2.lng)
                        min_distance = min(min_distance, distance)
            
            # If still infinity (no valid coordinates found), default to 0
            if min_distance == float('inf'):
                min_distance = 0.0
        
        # Create REAL nearby warehouses with actual distances
        nearby_warehouses = []
        for wh in warehouses_in_zipcode[:3]:  # Top 3 nearby
            distance = 0.0
            if len(warehouses_in_zipcode) > 1:
                # Calculate average distance to other warehouses in zipcode
                other_warehouses = [w for w in warehouses_in_zipcode if w.id != wh.id]
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
        
        # Calculate REAL scores based on actual data
        warehouse_count = len(warehouses_in_zipcode)
        total_requests_in_zipcode = data["totalRequests"]
        
        # Calculate coverage density score based on requests per warehouse
        avg_requests_per_warehouse = total_requests_in_zipcode / warehouse_count if warehouse_count > 0 else 0
        coverage_density_score = min(avg_requests_per_warehouse / 10.0, 1.0)  # Normalize by 10 requests per warehouse
        
        # Calculate population weighted gap score based on request density
        # Higher request density with fewer warehouses = higher gap score
        request_density = total_requests_in_zipcode / warehouse_count if warehouse_count > 0 else total_requests_in_zipcode
        population_weighted_gap_score = min(request_density / 20.0, 1.0)  # Normalize by 20 requests
        
        # Determine expansion opportunity based on REAL metrics
        # Priority: High > Moderate > None
        if avg_requests_per_warehouse > 25:  # Very high load per warehouse
            expansion_opportunity = "High"
        elif warehouse_count == 1 and total_requests_in_zipcode > 10:
            expansion_opportunity = "High"  # Single warehouse with high demand
        elif warehouse_count == 1 and total_requests_in_zipcode > 3:
            expansion_opportunity = "Moderate"  # Single warehouse with some demand
        elif warehouse_count < 3 and total_requests_in_zipcode > 15:
            expansion_opportunity = "Moderate"  # Few warehouses with high demand
        elif avg_requests_per_warehouse > 15:  # High load per warehouse
            expansion_opportunity = "Moderate"
        elif total_requests_in_zipcode == 0:
            expansion_opportunity = "None"  # No requests means no expansion needed
        else:
            expansion_opportunity = "None"
        
        # Calculate warehouses per 100 sq miles based on zipcode area estimation
        # Estimate zipcode area based on warehouse count and density
        estimated_zipcode_area_sq_miles = max(warehouse_count * 25, 50)  # Estimate 25 sq miles per warehouse, min 50
        warehouses_per_100_sq_miles = (warehouse_count / estimated_zipcode_area_sq_miles) * 100
        
        coverage_analysis.append(CoverageAnalysis(
            zipCode=data["zipCode"],
            city=data["city"],
            state=data["state"],
            population=int(50000 + total_requests_in_zipcode * 1000),  # Estimate population based on requests
            latitude=data["lat"],
            longitude=data["lng"],
            nearbyWarehouses=nearby_warehouses,
            minimumDistance=min_distance,
            warehouseCount=warehouse_count,
            coverageDensityScore=coverage_density_score,
            populationWeightedGapScore=population_weighted_gap_score,
            hasCoverageGap=warehouse_count < 2 or avg_requests_per_warehouse > 20,
            expansionOpportunity=expansion_opportunity,
            goldWarehouseCount=gold_count,
            silverWarehouseCount=silver_count,
            bronzeWarehouseCount=bronze_count,
            unTieredWarehouseCount=un_tiered_count,
            warehousesPer100SqMiles=warehouses_per_100_sq_miles,
            reqCount=total_requests_in_zipcode  # Total requests for this zipcode
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
    
    # Group warehouses by zipcode first, then expand with radius if provided (same logic as coverage gap)
    print("Grouping warehouses by zipcode for AI analysis")
    zipcode_warehouses_dict = {}
    for warehouse in static_warehouses:
        zipcode = warehouse.zipCode.strip() if warehouse.zipCode else ""
        if not zipcode:
            continue
        
        if zipcode not in zipcode_warehouses_dict:
            zipcode_warehouses_dict[zipcode] = {
                "city": warehouse.city,
                "state": warehouse.state,
                "warehouses": [],
                "totalRequests": 0
            }
        zipcode_warehouses_dict[zipcode]["warehouses"].append(warehouse)
        zipcode_warehouses_dict[zipcode]["totalRequests"] += warehouse.reqCount
    
    # If radius is provided, expand each zipcode group to include nearby warehouses (same as coverage gap)
    if radius_miles and radius_miles > 0:
        print(f"Expanding zipcode groups with radius: {radius_miles} miles for AI analysis")
        
        # Pre-filter warehouses with valid coordinates
        valid_warehouses = [wh for wh in static_warehouses if wh.lat != 0 and wh.lng != 0]
        print(f"  Processing {len(valid_warehouses)} warehouses with valid coordinates for AI analysis")
        
        for zipcode, data in zipcode_warehouses_dict.items():
            # Calculate center point of this zipcode group
            valid_coords = [(wh.lat, wh.lng) for wh in data["warehouses"] 
                          if wh.lat != 0 and wh.lng != 0]
            if not valid_coords:
                continue
            
            center_lat = sum(coord[0] for coord in valid_coords) / len(valid_coords)
            center_lng = sum(coord[1] for coord in valid_coords) / len(valid_coords)
            
            # Track which warehouses are already in this zipcode group
            existing_warehouse_ids = {wh.id for wh in data["warehouses"]}
            
            # Find all warehouses within radius of this zipcode's center
            # Only check warehouses with valid coordinates
            for warehouse in valid_warehouses:
                if warehouse.id in existing_warehouse_ids:
                    continue
                
                distance = haversine(center_lat, center_lng, warehouse.lat, warehouse.lng)
                
                if distance <= radius_miles:
                    data["warehouses"].append(warehouse)
                    data["totalRequests"] += warehouse.reqCount
                    existing_warehouse_ids.add(warehouse.id)
        
        print(f"  Radius expansion completed for AI analysis")
    
    # For AI analysis, pass unique warehouses only to avoid performance issues
    # Radius expansion creates too many duplicate entries (70k+) which causes the AI function to hang
    # The coverage gap endpoint still uses radius expansion, but AI analysis uses unique warehouses
    unique_warehouse_ids = set()
    unique_warehouses_list = []
    
    for zipcode, data in zipcode_warehouses_dict.items():
        for warehouse in data["warehouses"]:
            if warehouse.id not in unique_warehouse_ids:
                unique_warehouse_ids.add(warehouse.id)
                unique_warehouses_list.append(warehouse)
    
    print(f"Starting AI analysis for {len(unique_warehouses_list)} unique warehouses")
    if radius_miles and radius_miles > 0:
        print(f"  Note: Radius expansion ({radius_miles} miles) is applied to coverage gap endpoint but not AI analysis to avoid performance issues")
    ai_analysis = await analyze_coverage_gaps_with_ai(unique_warehouses_list, total_requests)
    print(f"AI analysis completed: {len(ai_analysis.coverageGaps)} gaps, {len(ai_analysis.recommendations)} recommendations")
    
    return ai_analysis

