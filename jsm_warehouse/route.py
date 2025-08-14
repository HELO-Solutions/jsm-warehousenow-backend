import json
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import redis

from geolocation.geolocation_service import get_distances
from jsm_warehouse.airtable_service import (
    fetch_warehouses_from_airtable,
    get_warehouse_by_id,
    search_warehouses_by_location,
    get_warehouse_stats
)

warehouse_router = APIRouter()

# Connect to Redis (optional for caching)
try:
    redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    REDIS_AVAILABLE = True
except:
    REDIS_AVAILABLE = False
    print("Redis not available, caching disabled")

CACHE_KEY = "warehouses_cache"
CACHE_TTL = 600 

class LocationRequest(BaseModel):
    postcode: str

class WarehouseSearchRequest(BaseModel):
    city: str = None
    state: str = None
    zip_code: str = None

@warehouse_router.get("/warehouses")
async def warehouses():
    """Get all warehouses from Airtable"""
    try:
        # Try to get from cache if Redis is available
        if REDIS_AVAILABLE:
            cached = redis_client.get(CACHE_KEY)
            if cached:
                return json.loads(cached)
        
        # Fetch from Airtable
        data = await fetch_warehouses_from_airtable()
        
        # Cache the result if Redis is available
        if REDIS_AVAILABLE:
            redis_client.setex(CACHE_KEY, CACHE_TTL, json.dumps(data))
        
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@warehouse_router.get("/warehouses/{warehouse_id}")
async def get_warehouse(warehouse_id: str):
    """Get a specific warehouse by ID"""
    try:
        warehouse = await get_warehouse_by_id(warehouse_id)
        return warehouse
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@warehouse_router.post("/warehouses/search")
async def search_warehouses(request: WarehouseSearchRequest):
    """Search warehouses by location criteria"""
    try:
        warehouses = await search_warehouses_by_location(
            city=request.city,
            state=request.state,
            zip_code=request.zip_code
        )
        return {"warehouses": warehouses, "count": len(warehouses)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@warehouse_router.post("/nearby_warehouses")
async def nearby_warehouses(request: LocationRequest):
    """Find nearest warehouses to a given postcode"""
    try:
        warehouses = await fetch_warehouses_from_airtable()
        if not warehouses:
            raise HTTPException(status_code=404, detail="No warehouses found")

        # Filter warehouses with coordinates (you'll need to implement geocoding)
        warehouses_with_coords = []
        for wh in warehouses:
            props = wh.get("properties", {})
            lat = props.get("latitude")
            lng = props.get("longitude")
            if lat is not None and lng is not None:
                warehouses_with_coords.append(wh)

        if not warehouses_with_coords:
            # For now, return warehouses without distance calculation
            return {
                "message": "No warehouses with coordinates found. Geocoding needed.",
                "warehouses": warehouses[:5]  # Return first 5 warehouses
            }

        # Create destinations for distance calculation
        destinations = []
        for wh in warehouses_with_coords:
            props = wh.get("properties", {})
            lat = props.get("latitude")
            lng = props.get("longitude")
            destinations.append(f"{lat},{lng}")

        # Calculate distances
        distances = await get_distances(request.postcode, destinations)
        
        # Zip warehouses with distances
        warehouse_distances = list(zip(warehouses_with_coords, distances))

        # Sort by distance ascending and pick top 5
        nearest = sorted(warehouse_distances, key=lambda x: x[1])[:5]

        # Add distance to each warehouse dict
        result = []
        for wh, dist in nearest:
            wh_copy = wh.copy()
            wh_copy["distance_meters"] = dist
            result.append(wh_copy)

        return {"nearest_warehouses": result}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@warehouse_router.get("/warehouses/stats/summary")
async def warehouse_stats():
    """Get warehouse statistics summary"""
    try:
        stats = await get_warehouse_stats()
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))