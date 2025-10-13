from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder
import httpx
import requests
import os
import time

from services.messaging.email_service import send_bulk_email
from services.geolocation.geolocation_service import get_coordinates_mapbox
from warehouse.models import LocationRequest, ResponseModel, SendBulkEmailData, SendEmailData
from warehouse.warehouse_service import fetch_orders_by_requestid_from_airtable, fetch_orders_from_airtable, fetch_warehouses_from_airtable, find_nearby_warehouses, invalidate_warehouse_cache, get_cache_status


warehouse_router = APIRouter()


@warehouse_router.get("/warehouses")
async def warehouses():
    try:
        data = await fetch_warehouses_from_airtable()
        total_records = len(data)
        return ResponseModel(
            status="success", 
            data={
                "warehouses": data,
                "total_records": total_records
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@warehouse_router.get("/requests")
async def requests(request_id: int):
    try:
        data = await fetch_orders_by_requestid_from_airtable(request_id=request_id)
        if not data:
            raise HTTPException(status_code=404, detail=f"Order with Request ID {request_id} not found")
        return ResponseModel(status="success", data=data)   
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    
@warehouse_router.get("/all-requests")
async def requests():
    try:
        data = await fetch_orders_from_airtable()
        if not data:
            raise HTTPException(status_code=404, detail=f"Orders not found")
        return ResponseModel(status="success", data=data)   
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    
@warehouse_router.post("/nearby_warehouses")
async def find_nearby_warehouses_endpoint(request: LocationRequest):
    try:
        nearby_warehouses = await find_nearby_warehouses(request.zip_code, request.radius_miles)
        encoded = jsonable_encoder(nearby_warehouses, exclude_none=False)
        return ResponseModel(status="success", data=encoded)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@warehouse_router.post("/send_email")
async def send_bulk_email_endpoint(send_bulk_emails: SendBulkEmailData):
    try:
        response = await send_bulk_email(send_bulk_emails)
        return ResponseModel(status="success", data=response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

# Cache management endpoints
@warehouse_router.post("/cache/refresh")
async def refresh_cache():
    """Manually refresh warehouse cache from Airtable."""
    try:
        warehouses = await fetch_warehouses_from_airtable(force_refresh=True)
        return ResponseModel(
            status="success", 
            data={
                "message": "Cache refreshed successfully",
                "warehouse_count": len(warehouses),
                "timestamp": time.time()
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache refresh failed: {str(e)}")

@warehouse_router.delete("/cache/clear")
async def clear_cache():
    """Clear all warehouse-related cache."""
    try:
        result = await invalidate_warehouse_cache()
        return ResponseModel(status="success", data=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache clear failed: {str(e)}")

@warehouse_router.get("/cache/status")
async def get_cache_status_endpoint():
    """Get detailed cache status and recommendations."""
    try:
        status = await get_cache_status()
        return ResponseModel(status="success", data=status)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get cache status: {str(e)}")


async def update_airtable_coordinates(record_id: str, latitude: float, longitude: float):
    """Update Airtable record with calculated coordinates."""
    try:
        airtable_token = os.getenv("AIRTABLE_TOKEN")
        base_id = os.getenv("BASE_ID")
        table_name = "Warehouses"
        
        url = f"https://api.airtable.com/v0/{base_id}/{table_name}/{record_id}"
        headers = {
            "Authorization": f"Bearer {airtable_token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "fields": {
                "Latitude": latitude,
                "Longitude": longitude
            }
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.patch(url, headers=headers, json=payload)
            response.raise_for_status()
            
        print(f"✅ Updated coordinates for record {record_id}: {latitude}, {longitude}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to update coordinates for record {record_id}: {str(e)}")
        return False

@warehouse_router.post("/webhook")
async def airtable_webhook(request: dict):
    """Handle Airtable webhook notifications for real-time cache invalidation and coordinate calculation."""
    try:
        print(f"📨 Webhook received: {request}")
        
        warehouse_data = request
        
        # Log the received data
        print(f"🏢 Warehouse data received: {warehouse_data.get('Warehouse Name', 'Unknown')}")
        
        # Clear cache when Airtable data changes
        await invalidate_warehouse_cache()
        
        # Check if coordinates need to be calculated
        zip_code = warehouse_data.get("ZIP")
        record_id = warehouse_data.get("Record ID")
        current_lat = warehouse_data.get("Latitude")
        current_lng = warehouse_data.get("Longitude")
        
        # Debug logging
        print(f"🔍 Debug - ZIP: {zip_code}, Record ID: {record_id}")
        print(f"🔍 Debug - Current Lat: {current_lat}, Current Lng: {current_lng}")
        print(f"🔍 Debug - Full warehouse data: {warehouse_data}")
        
        coordinate_update_result = None
        
        # Only calculate coordinates if:
        # 1. Warehouse has a ZIP code
        # 2. Either latitude or longitude is missing
        # 3. We have a valid record ID
        if zip_code and record_id and (not current_lat or not current_lng):
            print(f"🔍 Calculating coordinates for warehouse with ZIP: {zip_code}")
            
            try:
                # Get coordinates using Mapbox API
                coordinates = get_coordinates_mapbox(zip_code)
                
                if coordinates:
                    lat, lng = coordinates
                    print(f"📍 Coordinates found: {lat}, {lng}")
                    
                    # Update Airtable with new coordinates
                    update_success = await update_airtable_coordinates(record_id, lat, lng)
                    coordinate_update_result = {
                        "coordinates_calculated": True,
                        "latitude": lat,
                        "longitude": lng,
                        "airtable_updated": update_success
                    }
                else:
                    print(f"⚠️ No coordinates found for ZIP: {zip_code}")
                    coordinate_update_result = {
                        "coordinates_calculated": False,
                        "reason": "No coordinates found for ZIP code"
                    }
                    
            except Exception as coord_error:
                print(f"❌ Coordinate calculation error: {str(coord_error)}")
                coordinate_update_result = {
                    "coordinates_calculated": False,
                    "error": str(coord_error)
                }
        else:
            if not zip_code:
                print("ℹ️ No ZIP code provided, skipping coordinate calculation")
            elif current_lat and current_lng:
                print("ℹ️ Coordinates already exist, skipping calculation")
            else:
                print("ℹ️ No record ID provided, skipping coordinate calculation")
        
        return ResponseModel(
            status="success", 
            data={
                "message": "Webhook processed successfully",
                "warehouse_name": warehouse_data.get("Warehouse Name"),
                "cache_invalidated": True,
                "coordinate_update": coordinate_update_result,
                "timestamp": time.time()
            }
        )
            
    except Exception as e:
        print(f"❌ Webhook error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Webhook processing failed: {str(e)}")