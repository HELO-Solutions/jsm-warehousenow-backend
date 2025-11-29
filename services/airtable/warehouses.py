

from dotenv import load_dotenv
import httpx
import os

load_dotenv()
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("BASE_ID")
WAREHOUSE_TABLE_NAME = "Warehouses"

async def fetch_warehouses_from_airtable(force_refresh: bool = False) -> list[any]:
 
    from warehouse.warehouse_service import MemoryCache
    _cache = MemoryCache()
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

    ttl = 1800 if should_check else 3600  
    
    # Cache the result with Warehouse Master API view key
    _cache.set("warehouses:master_api", records, ttl=ttl)
    return records