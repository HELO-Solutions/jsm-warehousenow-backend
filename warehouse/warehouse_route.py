from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder
import httpx
import requests
import os
import time

from services.messaging.email_service import send_bulk_email
from warehouse.models import LocationRequest, ResponseModel, SendBulkEmailData, SendEmailData
from warehouse.warehouse_service import fetch_orders_by_requestid_from_airtable, fetch_orders_from_airtable, fetch_warehouses_from_airtable, find_nearby_warehouses, invalidate_warehouse_cache, get_cache_status


warehouse_router = APIRouter()


@warehouse_router.get("/warehouses")
async def warehouses():
    try:
        data = await fetch_warehouses_from_airtable()
        return ResponseModel(status="success", data=data)
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
        # Optional: Add admin authentication for production
        #     raise HTTPException(status_code=401, detail="Invalid admin key")
        
        # Force refresh by bypassing cache
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


@warehouse_router.post("/webhook")
async def airtable_webhook(request: dict):
    """Handle Airtable webhook notifications for real-time cache invalidation."""
    try:
        print(f"📨 Webhook received: {request}")
        
        # Optional: Add webhook authentication
        # webhook_secret = request.headers.get("X-Airtable-Webhook-Secret")
        # if webhook_secret != os.getenv("AIRTABLE_WEBHOOK_SECRET"):
        #     raise HTTPException(status_code=401, detail="Invalid webhook secret")
        
        # Process the warehouse data from Airtable
        warehouse_data = request
        
        # Log the received data
        print(f"🏢 Warehouse data received: {warehouse_data.get('Warehouse Name', 'Unknown')}")
        
        # Clear cache when Airtable data changes
        await invalidate_warehouse_cache()
        
        return ResponseModel(
            status="success", 
            data={
                "message": "Webhook processed successfully",
                "warehouse_name": warehouse_data.get("Warehouse Name"),
                "timestamp": time.time()
            }
        )
            
    except Exception as e:
        print(f"❌ Webhook error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Webhook processing failed: {str(e)}")