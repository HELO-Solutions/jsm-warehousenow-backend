from typing import List
from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder
import time

from services.messaging.email_service import send_bulk_email
from services.geolocation.geolocation_service import get_coordinates_google, update_airtable_coordinates
from services.slack_services.slack_service import export_warehouse_results_to_slack, get_channel_data_by_request
from warehouse.models import ChannelData, ExportWarehouseData, LocationRequest, ResponseModel, SendBulkEmailData, WarehouseData
from warehouse.warehouse_service import fetch_orders_by_requestid_from_airtable, fetch_orders_from_airtable, fetch_warehouses_from_airtable, find_nearby_warehouses, invalidate_warehouse_cache


warehouse_router = APIRouter(
    tags=["Warehouse"] 
)


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

@warehouse_router.post("/search/export")
async def export_search_to_slack(warehouses: List[ExportWarehouseData], zip: str, radius: str, request_id: str):
    try:
        canvas_id = await export_warehouse_results_to_slack(
            warehouses=warehouses,
            zip_searched=zip,
            radius=radius,
            request_id=request_id
        )
        return ResponseModel(status="success", data=canvas_id)

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@warehouse_router.post("/send_email")
async def send_bulk_email_endpoint(send_bulk_emails: SendBulkEmailData):
    try:
        response = await send_bulk_email(send_bulk_emails)
        return ResponseModel(status="success", data=response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@warehouse_router.post("/webhook")
async def airtable_webhook(request: dict):
    """Handle Airtable webhook notifications for real-time cache invalidation and coordinate calculation."""
    try:        
        warehouse_data = request
        
        await invalidate_warehouse_cache()
        
        zip_code = warehouse_data.get("ZIP")
        record_id = warehouse_data.get("Record ID")
        current_lat = warehouse_data.get("Latitude")
        current_lng = warehouse_data.get("Longitude")
               
        coordinate_update_result = None
        
        if zip_code and record_id and (not current_lat or not current_lng):
            print(f"🔍 Calculating coordinates for warehouse with ZIP: {zip_code}")
            
            try:
                coordinates = get_coordinates_google(zip_code)
                
                if coordinates:
                    lat, lng = coordinates
                    
                    update_success = await update_airtable_coordinates(record_id, lat, lng)
                    coordinate_update_result = {
                        "coordinates_calculated": True,
                        "latitude": lat,
                        "longitude": lng,
                        "airtable_updated": update_success
                    }
                else:
                    coordinate_update_result = {
                        "coordinates_calculated": False,
                        "reason": "No coordinates found for ZIP code"
                    }
                    
            except Exception as coord_error:
                coordinate_update_result = {
                    "coordinates_calculated": False,
                    "error": str(coord_error)
                }
        
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
        print(f"Webhook error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Webhook processing failed: {str(e)}")
