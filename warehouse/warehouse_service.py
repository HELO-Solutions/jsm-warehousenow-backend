import os
import re
import time
import asyncio
from typing import List, Optional, Dict, Any, Tuple
from threading import Lock

from pydantic import BaseModel
import httpx
import copy

from services.geolocation.geolocation_service import get_coordinates_mapbox, get_coordinates_google, get_driving_distance_and_time_google, get_coordinates_google_async
from warehouse.models import FilterWarehouseData, OrderData, WarehouseData
from services.gemini_services.ai_analysis import GENERAL_AI_ANALYSIS, analyze_warehouse_with_gemini
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
            keys_to_delete = [key for key in self._cache.keys() if key.startswith(('warehouses:', 'coords:', 'driving:'))]
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
            
            return {
                'total_entries': total_entries,
                'expired_entries': expired_entries,
                'active_entries': total_entries - expired_entries,
                'warehouse_entries': warehouse_entries,
                'last_airtable_check': self._last_airtable_check,
                'cache_age_hours': (current_time - self._last_airtable_check) / 3600
            }

# Global cache instance
_cache = MemoryCache()

class LocationRequest(BaseModel):
    zip_code: str
    radius_miles: float = 50  # default to 50 miles

# Optimized async functions with caching
async def get_coordinates_cached(zip_code: str) -> Optional[Tuple[float, float]]:
    """Get coordinates with caching."""
    cache_key = f"coords:{zip_code}"
    cached = _cache.get(cache_key)
    if cached:
        return cached
    
    coords = await get_coordinates_google_async(zip_code)
    if coords:
        _cache.set(cache_key, coords, ttl=86400)  # 24 hours
    return coords

async def get_driving_data_cached(origin_coords: Tuple[float, float], dest_coords: Tuple[float, float], origin_zip: str, dest_zip: str) -> Optional[Dict[str, float]]:
    """Get driving data with caching."""
    cache_key = f"driving:{origin_zip}:{dest_zip}"
    cached = _cache.get(cache_key)
    if cached:
        return cached
    
    result = await get_driving_distance_and_time_google(origin_coords, dest_coords)
    if result:
        _cache.set(cache_key, result, ttl=86400)  # 24 hours
    return result

async def batch_get_coordinates(zip_codes: List[str], max_concurrent: int = 10) -> Dict[str, Optional[Tuple[float, float]]]:
    """Get coordinates for multiple ZIP codes concurrently."""
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def get_single_coords(zip_code: str) -> Tuple[str, Optional[Tuple[float, float]]]:
        async with semaphore:
            coords = await get_coordinates_cached(zip_code)
            return zip_code, coords
    
    tasks = [get_single_coords(zip_code) for zip_code in zip_codes]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    coordinate_map = {}
    for result in results:
        if isinstance(result, Exception):
            continue
        zip_code, coords = result
        coordinate_map[zip_code] = coords
    
    return coordinate_map

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
        cached_warehouses = _cache.get("warehouses:all")
        if cached_warehouses:
            return cached_warehouses
    
    # Fetch fresh data from Airtable
    url = f"https://api.airtable.com/v0/{BASE_ID}/{WAREHOUSE_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}"
    }
    params = {
        # Removed view parameter to fetch all warehouses regardless of view state
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
    
    # Cache the result
    _cache.set("warehouses:all", records, ttl=ttl)
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

from services.geolocation.geolocation_service import (
    get_coordinates_mapbox,
    get_coordinates_google,
    get_driving_distance_and_time_google,
    haversine,
)

async def find_nearby_warehouses(origin_zip: str, radius_miles: float):
    """Optimized version with caching and batch processing."""
    origin_coords = get_coordinates_mapbox(origin_zip)
    if not origin_coords:
        return {"error": "Invalid ZIP code"}

    warehouses: List[WarehouseData] = await fetch_warehouses_from_airtable()
    
    # Extract unique ZIP codes and filter warehouses
    unique_zips = set()
    valid_warehouses = []
    
    for wh in warehouses:
        wh_zip = wh["fields"].get("ZIP")
        if wh_zip:
            unique_zips.add(wh_zip)
            valid_warehouses.append(wh)
    
    # Batch get coordinates for all unique ZIP codes
    zip_coords_map = await batch_get_coordinates(list(unique_zips), max_concurrent=10)
    
    # Pre-filter warehouses using Haversine distance
    candidate_warehouses = []
    for wh in valid_warehouses:
        wh_zip = wh["fields"].get("ZIP")
        wh_coords = zip_coords_map.get(wh_zip)
        
        if not wh_coords:
            continue
            
        # Fast pre-filter with Haversine
        straight_line_miles = haversine(
            origin_coords[0], origin_coords[1],
            wh_coords[0], wh_coords[1]
        )
        
        if straight_line_miles <= radius_miles * 2:  # 2x buffer for driving distance
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
            wh_copy["warehouse_id"] = wh["fields"]["WarehouseID"]
            
            nearby.append(wh_copy)
    
    # Sort final list
    nearby.sort(key=lambda x: (x["tier_rank"], x["duration_minutes"], x["distance_miles"]))

    # AI analysis with fallback
    try:
        ai_analysis = await analyze_warehouse_with_gemini(nearby)
    except Exception:
        ai_analysis = GENERAL_AI_ANALYSIS

    return {"origin_zip": origin_zip, "warehouses": nearby, "ai_analysis": ai_analysis}

import httpx

async def fetch_orders_by_requestid_from_airtable(request_id: int) -> List[OrderData]:
    url = f"https://api.airtable.com/v0/{BASE_ID}/{ODER_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}"
    }
    params = {
        "filterByFormula": f"{{Request ID}} = {request_id}",
        # Removed view parameter to fetch all orders regardless of view state
    }

    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

    records = data.get("records", [])
    if not records:
        return []  # return empty list if no matches

    results: List[OrderData] = []

    for record in records:
        fields = record.get("fields", {})

        request_images: List[str] = []
        raw_images = fields.get("BOL & Pictures")

        if isinstance(raw_images, str):
            # Case: "filename (url), filename (url)"
            request_images = re.findall(r"\((https?://[^\)]+)\)", raw_images)

        elif isinstance(raw_images, list):
            # Case: array of objects with "url"
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
    params = {
        # Removed view parameter to fetch all orders regardless of view state
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
    
    return records