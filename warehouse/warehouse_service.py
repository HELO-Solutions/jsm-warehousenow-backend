"""
Updated warehouse service that uses latitude and longitude from Airtable.
This eliminates the need for coordinate API calls, saving significant costs.
"""

import os
import re
import time
import asyncio
import re
from typing import List, Optional, Dict, Any, Tuple
from threading import Lock

from pydantic import BaseModel
import httpx
import copy

from services.geolocation.geolocation_service import get_coordinates_mapbox, get_driving_distance_and_time_google, haversine
from warehouse.models import FilterWarehouseData, OrderData, WarehouseData, StaticWarehouseData, CoverageAnalysisResponse, CoverageAnalysis, AIAnalysisData, CoverageGap, HighRequestArea, RequestTrends, Recommendation, MockWarehouse, CoverageGapFilters
from services.gemini_services.ai_analysis import GENERAL_AI_ANALYSIS, analyze_warehouse_with_gemini
from services.gemini_services.coverage_gap_analysis import analyze_coverage_gaps_with_ai, analyze_coverage_gaps_without_ai
from dotenv import load_dotenv

load_dotenv()
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("BASE_ID")
WAREHOUSE_TABLE_NAME = "Warehouses" 
ODER_TABLE_NAME = "Requests"

# In-memory cache for performance optimization
class MemoryCache:
    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()
        self._last_airtable_check = 0
        self._airtable_check_interval = 300  
    
    def _is_expired(self, entry: Dict[str, Any]) -> bool:
        return time.time() > entry.get('expires_at', 0)
    
    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                if not self._is_expired(entry):
                    return entry['value']
                else:
                    del self._cache[key]
            return None
    
    def set(self, key: str, value: Any, ttl: int = 3600) -> None:
        with self._lock:
            self._cache[key] = {
                'value': value,
                'expires_at': time.time() + ttl,
                'created_at': time.time()
            }
    
    def delete(self, key: str) -> None:
        """Delete a specific cache entry."""
        with self._lock:
            self._cache.pop(key, None)
    
    def clear_warehouse_cache(self) -> None:
        """Clear all warehouse-related cache entries."""
        with self._lock:
            keys_to_delete = [key for key in self._cache.keys() if key.startswith(('warehouses:', 'driving:', 'requests:', 'coverage_gap:'))]
            for key in keys_to_delete:
                del self._cache[key]
    
    def should_check_airtable(self) -> bool:
        """Check if we should verify Airtable for updates."""
        current_time = time.time()
        if current_time - self._last_airtable_check > self._airtable_check_interval:
            self._last_airtable_check = current_time
            return True
        return False
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for monitoring."""
        with self._lock:
            current_time = time.time()
            total_entries = len(self._cache)
            expired_entries = sum(1 for entry in self._cache.values() if self._is_expired(entry))
            warehouse_entries = sum(1 for key in self._cache.keys() if key.startswith('warehouses:'))
            driving_entries = sum(1 for key in self._cache.keys() if key.startswith('driving:'))
            request_entries = sum(1 for key in self._cache.keys() if key.startswith('requests:'))
            coverage_gap_entries = sum(1 for key in self._cache.keys() if key.startswith('coverage_gap:'))
            
            return {
                'total_entries': total_entries,
                'expired_entries': expired_entries,
                'active_entries': total_entries - expired_entries,
                'warehouse_entries': warehouse_entries,
                'driving_entries': driving_entries,
                'request_entries': request_entries,
                'coverage_gap_entries': coverage_gap_entries,
                'last_airtable_check': self._last_airtable_check,
                'cache_age_hours': (current_time - self._last_airtable_check) / 3600
            }

# Global cache instance
_cache = MemoryCache()

class LocationRequest(BaseModel):
    zip_code: str
    radius_miles: float = 50  # default to 50 miles

async def get_driving_data_cached(origin_coords: Tuple[float, float], dest_coords: Tuple[float, float], origin_zip: str, dest_zip: str) -> Optional[Dict[str, float]]:
    """Get driving data with bidirectional caching."""
    # Create consistent cache key regardless of direction
    cache_key = get_driving_cache_key(origin_zip, dest_zip)
    cached = _cache.get(cache_key)
    if cached:
        return cached
    
    result = await get_driving_distance_and_time_google(origin_coords, dest_coords)
    if result:
        _cache.set(cache_key, result, ttl=86400)  # 24 hours
    return result

def get_driving_cache_key(origin_zip: str, dest_zip: str) -> str:
    """Generate consistent cache key for bidirectional routes."""
    sorted_zips = sorted([origin_zip, dest_zip])
    return f"driving:{sorted_zips[0]}:{sorted_zips[1]}"

async def batch_get_driving_data(origin_coords: Tuple[float, float], dest_coords_list: List[Tuple[float, float]], origin_zip: str, dest_zips: List[str], max_concurrent: int = 5) -> List[Optional[Dict[str, float]]]:
    """Get driving data for multiple destinations concurrently."""
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def get_single_driving(dest_coords: Tuple[float, float], dest_zip: str) -> Optional[Dict[str, float]]:
        async with semaphore:
            return await get_driving_data_cached(origin_coords, dest_coords, origin_zip, dest_zip)
    
    tasks = []
    for i, dest_coords in enumerate(dest_coords_list):
        dest_zip = dest_zips[i] if i < len(dest_zips) else None
        tasks.append(get_single_driving(dest_coords, dest_zip))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    driving_data_list = []
    for result in results:
        if isinstance(result, Exception):
            driving_data_list.append(None)
        else:
            driving_data_list.append(result)
    
    return driving_data_list

async def fetch_warehouses_from_airtable(force_refresh: bool = False) -> list[any]:
    """Fetch warehouses with smart caching and invalidation strategies."""
    
    # Check if we should verify Airtable for updates
    should_check = _cache.should_check_airtable() or force_refresh
    
    if not should_check:
        # Return cached data if available and not expired
        cached_warehouses = _cache.get("warehouses:master_api")
        if cached_warehouses:
            return cached_warehouses
    
    # Fetch fresh data from Airtable 
    url = f"https://api.airtable.com/v0/{BASE_ID}/{WAREHOUSE_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}"
    }
    params = {
        
    }

    records = []
    async with httpx.AsyncClient() as client:
        offset = None
        while True:
            if offset:
                params["offset"] = offset
            resp = await client.get(url, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json()
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break

    # Smart TTL based on data freshness
    # Shorter TTL for more frequent checks, longer for stable data
    ttl = 1800 if should_check else 3600  # 30 min vs 1 hour
    
    # Cache the result with Warehouse Master API view key
    _cache.set("warehouses:master_api", records, ttl=ttl)
    return records

async def invalidate_warehouse_cache() -> Dict[str, Any]:
    """Manually invalidate warehouse cache."""
    _cache.clear_warehouse_cache()
    return {"status": "success", "message": "Warehouse cache cleared"}

async def get_cache_status() -> Dict[str, Any]:
    """Get detailed cache status for monitoring."""
    stats = _cache.get_cache_stats()
    return {
        "cache_stats": stats,
        "recommendations": _get_cache_recommendations(stats)
    }

def _get_cache_recommendations(stats: Dict[str, Any]) -> List[str]:
    """Get cache optimization recommendations."""
    recommendations = []
    
    if stats['cache_age_hours'] > 2:
        recommendations.append("Consider refreshing cache - data is over 2 hours old")
    
    if stats['expired_entries'] > stats['active_entries']:
        recommendations.append("High expired entries - consider shorter TTL")
    
    if stats['warehouse_entries'] == 0:
        recommendations.append("No warehouse data cached - may need manual refresh")
    
    return recommendations

# Find nearby warehouses (openStreetMap)
def _tier_rank(tier: str) -> int:
    """Lower number = higher priority."""
    if not tier:
        return 99
    t = str(tier).strip().lower()
    order = {"gold": 0, "silver": 1, "bronze": 2}
    return order.get(t, 99)

def find_missing_fields(fields: dict) -> List[str]:
    """Return a list of field names that are empty or missing compared to FilterWarehouseData"""
    missing = []
    for field_name in FilterWarehouseData.model_fields.keys():
        value = fields.get(field_name)
        if value in (None, "", [], {}):
            missing.append(field_name)
    return missing

async def find_nearby_warehouses(origin_zip: str, radius_miles: float):
    """Optimized version using lat/lng from Airtable - no coordinate API calls needed!"""
    origin_coords = get_coordinates_mapbox(origin_zip)
    if not origin_coords:
        return {"origin_zip": origin_zip, "warehouses": [], "ai_analysis": GENERAL_AI_ANALYSIS, "error": "Invalid ZIP code"}

    warehouses: List[WarehouseData] = await fetch_warehouses_from_airtable()
    
    # Direct Haversine calculation using lat/lng from Airtable (no API calls!)
    haversine_filtered_warehouses = []
    warehouses_with_coords = 0
    warehouses_without_coords = 0
    
    for wh in warehouses:
        lat = wh["fields"].get("Latitude")
        lng = wh["fields"].get("Longitude")
        
        if lat and lng:
            warehouses_with_coords += 1
            # Direct Haversine calculation (no API calls!)
            straight_line_miles = haversine(
                origin_coords[0], origin_coords[1],
                float(lat), float(lng)
            )
            
            # Use 2x buffer for Haversine pre-filtering
            if straight_line_miles <= radius_miles * 2:
                haversine_filtered_warehouses.append({
                    'warehouse': wh,
                    'coordinates': (float(lat), float(lng)),
                    'zip': wh["fields"].get("ZIP"),
                    'haversine_distance': straight_line_miles
                })
        else:
            warehouses_without_coords += 1
    
    print(f"📊 Coordinate analysis: {warehouses_with_coords} with coordinates, {warehouses_without_coords} without")
    print(f"🎯 Haversine pre-filtering: {len(warehouses)} → {len(haversine_filtered_warehouses)} warehouses")
    
    if not haversine_filtered_warehouses:
        return {"origin_zip": origin_zip, "warehouses": [], "ai_analysis": GENERAL_AI_ANALYSIS}
    
    # Process Haversine-filtered warehouses for driving distance calculation
    candidate_warehouses = []
    for item in haversine_filtered_warehouses:
        wh = item['warehouse']
        wh_coords = item['coordinates']
        wh_zip = item['zip']
        
        # No need for additional haversine filtering - already filtered above
        candidate_warehouses.append({
            'warehouse': wh,
            'coordinates': wh_coords,
            'zip': wh_zip
        })
    
    if not candidate_warehouses:
        return {"origin_zip": origin_zip, "warehouses": [], "ai_analysis": GENERAL_AI_ANALYSIS}
    
    # Batch get driving data for candidates
    dest_coords_list = [candidate['coordinates'] for candidate in candidate_warehouses]
    dest_zips = [candidate['zip'] for candidate in candidate_warehouses]
    
    driving_results = await batch_get_driving_data(
        origin_coords, 
        dest_coords_list,
        origin_zip,
        dest_zips,
        max_concurrent=5
    )
    
    # Process results and build final list
    nearby: List[WarehouseData] = []
    for i, candidate in enumerate(candidate_warehouses):
        driving_data = driving_results[i]
        if not driving_data:
            continue
            
        distance_miles = driving_data["distance_miles"]
        duration_minutes = driving_data["duration_minutes"]
        
        if distance_miles <= radius_miles:
            wh = candidate['warehouse']
            wh_copy = copy.copy(wh)
            wh_copy["distance_miles"] = distance_miles
            wh_copy["duration_minutes"] = duration_minutes
            wh_copy["tier_rank"] = _tier_rank(wh["fields"].get("Tier"))
            wh_copy["tags"] = find_missing_fields(wh["fields"])
            wh_copy["has_missed_fields"] = bool(wh_copy["tags"])
            wh_copy["warehouse_id"] = wh["fields"].get("WarehouseID", "")
            
            nearby.append(wh_copy)
    
    # Sort final list
    nearby.sort(key=lambda x: (x["tier_rank"], x["duration_minutes"], x["distance_miles"]))

    # Debug: Check for any objects that might cause React issues
    for i, warehouse in enumerate(nearby):
        for key, value in warehouse.items():
            if isinstance(value, dict) and key != "fields":
                print(f"⚠️ Warning: Warehouse {i} has object field '{key}': {value}")
            elif isinstance(value, list) and any(isinstance(item, dict) for item in value):
                print(f"⚠️ Warning: Warehouse {i} has list with objects in field '{key}': {value}")

    # AI analysis with fallback
    try:
        ai_analysis = await analyze_warehouse_with_gemini(nearby)
    except Exception:
        ai_analysis = GENERAL_AI_ANALYSIS

    return {"origin_zip": origin_zip, "warehouses": nearby, "ai_analysis": ai_analysis}

# Additional functions for orders (unchanged)
async def fetch_orders_by_requestid_from_airtable(request_id: int) -> List[OrderData]:
    url = f"https://api.airtable.com/v0/{BASE_ID}/{ODER_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}"
    }
    params = {
        "filterByFormula": f"{{Request ID}} = {request_id}",
    }

    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

    records = data.get("records", [])
    if not records:
        return []

    results: List[OrderData] = []
    for record in records:
        fields = record.get("fields", {})
        request_images: List[str] = []
        raw_images = fields.get("BOL & Pictures")

        if isinstance(raw_images, str):
            request_images = re.findall(r"\((https?://[^\)]+)\)", raw_images)
        elif isinstance(raw_images, list):
            for img in raw_images:
                if isinstance(img, dict) and "url" in img:
                    request_images.append(img["url"])

        results.append(
            OrderData(
                commodity=fields.get("Commodity"),
                loading_method=fields.get("Loading Style"),
                request_images=request_images
            )
        )

    return results

async def fetch_orders_from_airtable():
    url = f"https://api.airtable.com/v0/{BASE_ID}/{ODER_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}"
    }
    params = {}

    records = []
    async with httpx.AsyncClient() as client:
        offset = None
        while True:
            if offset:
                params["offset"] = offset
            resp = await client.get(url, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json()
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
    
    return records

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
    from datetime import datetime, timezone
    import math
    
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

from typing import List

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


