
import time
import asyncio
from typing import List, Optional, Dict, Any, Tuple
from threading import Lock
import copy
from services.airtable.warehouses import fetch_warehouses_from_airtable
from services.geolocation.geolocation_service import get_coordinates_mapbox, get_driving_distance_and_time_google, haversine
from warehouse.models import FilterWarehouseData, WarehouseData
from services.gemini_services.ai_analysis import GENERAL_AI_ANALYSIS, analyze_warehouse_with_gemini

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
    
    def clear_warehouse_cache(self) -> None:
        with self._lock:
            keys_to_delete = [key for key in self._cache.keys() if key.startswith(('warehouses:', 'driving:', 'requests:'))]
            for key in keys_to_delete:
                del self._cache[key]
    
    def should_check_airtable(self) -> bool:
        """Check if we should verify Airtable for updates."""
        current_time = time.time()
        if current_time - self._last_airtable_check > self._airtable_check_interval:
            self._last_airtable_check = current_time
            return True
        return False
    
# Global cache instance
_cache = MemoryCache() 

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

async def invalidate_warehouse_cache() -> Dict[str, Any]:
    _cache.clear_warehouse_cache()
    return {"status": "success", "message": "Warehouse cache cleared"}


def _tier_rank(tier: str) -> int:
    if not tier:
        return 99
    t = str(tier).strip().lower()
    order = {"gold": 0, "silver": 1, "bronze": 2}
    return order.get(t, 99)

def find_missing_fields(fields: dict) -> List[str]:
    missing = []
    for field_name in FilterWarehouseData.model_fields.keys():
        value = fields.get(field_name)
        if value in (None, "", [], {}):
            missing.append(field_name)
    return missing

async def find_nearby_warehouses(origin_zip: str, radius_miles: float):
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

    try:
        ai_analysis = await analyze_warehouse_with_gemini(nearby)
    except Exception:
        ai_analysis = GENERAL_AI_ANALYSIS

    return {"origin_zip": origin_zip, "warehouses": nearby, "ai_analysis": ai_analysis}