"""
Script to find requests in the Requests table that have null or empty warehouse IDs.
"""

import os
import asyncio
import httpx
from dotenv import load_dotenv

load_dotenv()

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("BASE_ID")
REQUESTS_TABLE_NAME = "Requests"


async def check_null_warehouse_requests():
    """Check for requests with null or empty warehouse IDs."""
    
    if not AIRTABLE_TOKEN or not BASE_ID:
        print("ERROR: AIRTABLE_TOKEN or BASE_ID not found in environment variables")
        return
    
    url = f"https://api.airtable.com/v0/{BASE_ID}/{REQUESTS_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}"
    }
    params = {}
    
    requests_without_warehouse = []
    total_requests = 0
    
    print("Fetching requests from Airtable...")
    
    async with httpx.AsyncClient() as client:
        offset = None
        while True:
            if offset:
                params["offset"] = offset
            
            try:
                resp = await client.get(url, headers=headers, params=params)
                resp.raise_for_status()
                data = resp.json()
                
                records = data.get("records", [])
                total_requests += len(records)
                
                for record in records:
                    record_id = record.get("id", "")
                    fields = record.get("fields", {})
                    
                    # Get the Warehouse field (it's a list type in Airtable)
                    warehouse_field = fields.get("Warehouse", [])
                    
                    # Check if warehouse is null, empty, or empty list
                    if not warehouse_field or len(warehouse_field) == 0:
                        request_id = fields.get("Request ID", "N/A")
                        city = fields.get("City", "N/A")
                        state = fields.get("State", "N/A")
                        created_time = fields.get("Created Time", "N/A")
                        
                        requests_without_warehouse.append({
                            "record_id": record_id,
                            "request_id": request_id,
                            "city": city,
                            "state": state,
                            "created_time": created_time,
                            "warehouse_value": warehouse_field
                        })
                
                offset = data.get("offset")
                if not offset:
                    break
                    
            except httpx.HTTPStatusError as e:
                print(f"HTTP Error: {e.response.status_code} - {e.response.text}")
                break
            except Exception as e:
                print(f"Error fetching requests: {e}")
                break
    
    # Print results
    print("\n" + "="*80)
    print(f"RESULTS: Requests with null or empty Warehouse ID")
    print("="*80)
    print(f"Total requests checked: {total_requests}")
    print(f"Requests without warehouse: {len(requests_without_warehouse)}")
    print("="*80)
    
    if requests_without_warehouse:
        print("\nRequests without warehouse ID:")
        print("-" * 80)
        for i, req in enumerate(requests_without_warehouse, 1):
            print(f"\n{i}. Request ID: {req['request_id']}")
            print(f"   Record ID: {req['record_id']}")
            print(f"   City: {req['city']}")
            print(f"   State: {req['state']}")
            print(f"   Created Time: {req['created_time']}")
            print(f"   Warehouse Value: {req['warehouse_value']}")
    else:
        print("\n✓ No requests found with null or empty warehouse IDs!")
    
    print("\n" + "="*80)
    
    return requests_without_warehouse


if __name__ == "__main__":
    asyncio.run(check_null_warehouse_requests())




