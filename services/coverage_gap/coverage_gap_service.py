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
        zipCode=safe_string_field(fields.get("ZIP", "")),
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
        if filters.tier and wh.tier not in filters.tier:
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
        return cached
    
    print("=== COVERAGE GAP ANALYSIS STARTED ===")
    
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
    
    # Group warehouses by zipcode OR by radius for analysis
    if radius_miles and radius_miles > 0:
        print(f"Grouping warehouses by radius: {radius_miles} miles")
        # Group by radius-based clustering
        location_groups = []
        unprocessed_warehouses = static_warehouses.copy()
        
        while unprocessed_warehouses:
            # Start a new cluster with the first warehouse
            cluster_center = unprocessed_warehouses.pop(0)
            cluster = [cluster_center]
            
            # Find all warehouses within radius
            remaining = []
            for wh in unprocessed_warehouses:
                if cluster_center.lat != 0 and cluster_center.lng != 0 and wh.lat != 0 and wh.lng != 0:
                    distance = haversine(cluster_center.lat, cluster_center.lng, wh.lat, wh.lng)
                    if distance <= radius_miles:
                        cluster.append(wh)
                    else:
                        remaining.append(wh)
                else:
                    remaining.append(wh)
            
            unprocessed_warehouses = remaining
            
            # Create analysis group for this cluster
            if cluster:
                # Use the first warehouse's location as the group center
                location_groups.append({
                    "city": cluster[0].city,
                    "state": cluster[0].state,
                    "zipCode": cluster[0].zipCode or f"cluster_{len(location_groups)}",
                    "warehouses": cluster,
                    "lat": cluster[0].lat,
                    "lng": cluster[0].lng,
                    "totalRequests": sum(w.reqCount for w in cluster)
                })
        
        zipcode_analysis = {group["zipCode"]: group for group in location_groups}
    else:
        # Group warehouses by zipcode for analysis (more granular than city/state)
        print("Grouping warehouses by zipcode")
        zipcode_analysis = {}
        for warehouse in static_warehouses:
            zipcode = warehouse.zipCode
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
    
    # Create coverage analysis for each zipcode with REAL calculations
    for zipcode, data in zipcode_analysis.items():
        warehouses_in_zipcode = data["warehouses"]
        
        # Count warehouses by tier (REAL data)
        gold_count = sum(1 for w in warehouses_in_zipcode if w.tier == "Gold")
        silver_count = sum(1 for w in warehouses_in_zipcode if w.tier == "Silver")
        bronze_count = sum(1 for w in warehouses_in_zipcode if w.tier == "Bronze")
        
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


async def get_ai_analysis_only(filters: Optional[CoverageGapFilters] = None) -> AIAnalysisData:
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
    
    # Get AI analysis
    print(f"Starting AI analysis for {len(static_warehouses)} warehouses")
    ai_analysis = await analyze_coverage_gaps_with_ai(static_warehouses, total_requests)
    print(f"AI analysis completed: {len(ai_analysis.coverageGaps)} gaps, {len(ai_analysis.recommendations)} recommendations")
    
    return ai_analysis

