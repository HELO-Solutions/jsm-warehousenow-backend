#!/usr/bin/env python3
"""
Script to add latitude and longitude fields to Airtable and populate them with coordinates.
This will eliminate the need for coordinate API calls in the main application.
"""

import os
import asyncio
import httpx
from dotenv import load_dotenv
from services.geolocation.geolocation_service import get_coordinates_google_async

load_dotenv()

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
BASE_ID = os.getenv("BASE_ID")
WAREHOUSE_TABLE_NAME = "Warehouses"

async def check_lat_lng_fields():
    """Check if latitude and longitude fields exist in Airtable table schema."""
    print("Checking for latitude and longitude fields in Airtable table schema...")
    
    try:
        # Get table schema to check for field existence
        url = f"https://api.airtable.com/v0/meta/bases/{BASE_ID}/tables"
        headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            # Find the Warehouses table
            warehouse_table = None
            for table in data.get("tables", []):
                if table["name"] == WAREHOUSE_TABLE_NAME:
                    warehouse_table = table
                    break
            
            if not warehouse_table:
                print("ERROR: Warehouses table not found")
                return False
            
            # Check if fields exist in table schema
            table_fields = [field["name"] for field in warehouse_table.get("fields", [])]
            has_latitude = "Latitude" in table_fields
            has_longitude = "Longitude" in table_fields
            
            print(f"Available fields in table schema:")
            for field_name in sorted(table_fields):
                print(f"   - {field_name}")
            
            if has_latitude and has_longitude:
                print("SUCCESS: Latitude and Longitude fields exist in table schema")
                return True
            else:
                print("ERROR: Latitude and Longitude fields are missing from table schema")
                print("Please manually add these fields to your Airtable:")
                print("   1. Go to your Airtable base")
                print("   2. Open the 'Warehouses' table")
                print("   3. Add these fields:")
                print("      - 'Latitude' (Number field, 6 decimal places)")
                print("      - 'Longitude' (Number field, 6 decimal places)")
                print("   4. Make sure they're visible in the 'Warehouse Master API' view")
                print("   5. Run this script again")
                return False
                
    except Exception as e:
        print(f"ERROR: Error checking table schema: {e}")
        print("Please manually add these fields to your Airtable:")
        print("   1. Go to your Airtable base")
        print("   2. Open the 'Warehouses' table")
        print("   3. Add these fields:")
        print("      - 'Latitude' (Number field, 6 decimal places)")
        print("      - 'Longitude' (Number field, 6 decimal places)")
        print("   4. Make sure they're visible in the 'Warehouse Master API' view")
        print("   5. Run this script again")
        return False

async def get_all_warehouses():
    """Fetch all warehouses from Airtable Warehouse Master API view."""
    print("Fetching all warehouses from Warehouse Master API view...")
    
    url = f"https://api.airtable.com/v0/{BASE_ID}/{WAREHOUSE_TABLE_NAME}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    
    all_warehouses = []
    async with httpx.AsyncClient() as client:
        offset = None
        while True:
            params = {"view": "Warehouse Master API"}
            if offset:
                params["offset"] = offset
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            all_warehouses.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
    
    print(f"Found {len(all_warehouses)} warehouses")
    return all_warehouses

async def get_coordinates_for_warehouse(warehouse):
    """Get coordinates for a single warehouse."""
    zip_code = warehouse["fields"].get("ZIP")
    if not zip_code:
        return None, None
    
    try:
        coords = await get_coordinates_google_async(zip_code)
        if coords:
            return coords[0], coords[1]  # lat, lng
    except Exception as e:
        print(f"⚠️ Error getting coordinates for ZIP {zip_code}: {e}")
    
    return None, None

async def update_warehouse_coordinates(warehouse_id, lat, lng):
    """Update a warehouse record with coordinates."""
    url = f"https://api.airtable.com/v0/{BASE_ID}/{WAREHOUSE_TABLE_NAME}/{warehouse_id}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json"
    }
    
    update_data = {
        "fields": {
            "Latitude": lat,
            "Longitude": lng
        }
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.patch(url, headers=headers, json=update_data)
        response.raise_for_status()
        return response.json()

async def populate_coordinates():
    """Main function to populate coordinates for all warehouses."""
    print("Starting coordinate population process...")
    
    # Step 1: Check if fields exist
    fields_exist = await check_lat_lng_fields()
    if not fields_exist:
        print("ERROR: Required fields are missing. Please add them manually first.")
        return
    
    # Step 2: Get all warehouses
    warehouses = await get_all_warehouses()
    
    # Step 3: Process warehouses in batches
    batch_size = 10
    total_warehouses = len(warehouses)
    processed = 0
    updated = 0
    failed = 0
    
    print(f"Processing {total_warehouses} warehouses in batches of {batch_size}...")
    
    for i in range(0, total_warehouses, batch_size):
        batch = warehouses[i:i + batch_size]
        
        # Process batch concurrently
        tasks = []
        for warehouse in batch:
            task = process_warehouse(warehouse)
            tasks.append(task)
        
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Update Airtable with results
        for j, result in enumerate(batch_results):
            warehouse = batch[j]
            processed += 1
            
            if isinstance(result, Exception):
                print(f"❌ Error processing warehouse {warehouse['id']}: {result}")
                failed += 1
            elif result:
                lat, lng = result
                try:
                    await update_warehouse_coordinates(warehouse['id'], lat, lng)
                    updated += 1
                    print(f"SUCCESS: Updated warehouse {warehouse['id']} with coordinates ({lat}, {lng})")
                except Exception as e:
                    print(f"ERROR: Failed to update warehouse {warehouse['id']}: {e}")
                    failed += 1
            else:
                print(f"WARNING: No coordinates found for warehouse {warehouse['id']}")
                failed += 1
        
        # Progress update
        progress = (processed / total_warehouses) * 100
        print(f"Progress: {processed}/{total_warehouses} ({progress:.1f}%) - Updated: {updated}, Failed: {failed}")
    
    print(f"\nCoordinate population complete!")
    print(f"Final stats:")
    print(f"   - Total warehouses: {total_warehouses}")
    print(f"   - Successfully updated: {updated}")
    print(f"   - Failed: {failed}")
    print(f"   - Success rate: {(updated/total_warehouses)*100:.1f}%")

async def process_warehouse(warehouse):
    """Process a single warehouse to get coordinates."""
    return await get_coordinates_for_warehouse(warehouse)

async def main():
    """Main execution function."""
    print("Airtable Coordinate Population Script")
    print("=" * 50)
    
    # Check environment variables
    if not AIRTABLE_TOKEN:
        print("ERROR: AIRTABLE_TOKEN not found in environment variables")
        return
    
    if not BASE_ID:
        print("ERROR: BASE_ID not found in environment variables")
        return
    
    try:
        await populate_coordinates()
    except Exception as e:
        print(f"ERROR: Script failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
