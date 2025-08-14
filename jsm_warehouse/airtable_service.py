import os
import json
from typing import List, Dict, Any
from fastapi import HTTPException
from pyairtable import Api, Table

# Airtable Configuration
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "appqsjdQHyqZ5OMrx")
AIRTABLE_TABLE_NAME = "Warehouses"

# Initialize Airtable API
api = Api(AIRTABLE_API_KEY)
table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

async def fetch_warehouses_from_airtable() -> List[Dict[str, Any]]:
    """Fetch warehouses from Airtable"""
    try:
        # Get all records from the Warehouses table
        records = table.all()
        
        # Transform Airtable records to match your expected format
        warehouses = []
        for record in records:
            warehouse = {
                "id": record["id"],
                "properties": {
                    "name": record["fields"].get("Name", ""),
                    "city": record["fields"].get("City", ""),
                    "state": record["fields"].get("State", ""),
                    "zip": record["fields"].get("Zip", ""),
                    "full_address": record["fields"].get("Full Address", ""),
                    "status": record["fields"].get("Status", ""),
                    "tier": record["fields"].get("Tier", ""),
                    "contact_1": record["fields"].get("Contact 1", ""),
                    "email_1": record["fields"].get("Email 1", ""),
                    "office_phone": record["fields"].get("Office #", ""),
                    "cell_phone": record["fields"].get("Cell # 1", ""),
                    "contact_2": record["fields"].get("Contact 2", ""),
                    "cell_phone_2": record["fields"].get("Cell # 2", ""),
                    "email_2": record["fields"].get("Email 2", ""),
                    "email_3": record["fields"].get("Email 3", ""),
                    "hours": record["fields"].get("Hours", ""),
                    "hazmat": record["fields"].get("Hazmat", ""),
                    "temp_control": record["fields"].get("Temp Control", ""),
                    "food_grade": record["fields"].get("Food Grade", ""),
                    "paper_rolls": record["fields"].get("Paper Rolls", False),
                    "services": record["fields"].get("Services", []),
                    "website": record["fields"].get("Website", ""),
                    "notes_pricing": record["fields"].get("Notes / Pricing", ""),
                    "insurance": record["fields"].get("Insurance", ""),
                    # Add coordinates for geolocation (you'll need to geocode addresses)
                    "latitude": None,  # Will be populated by geocoding service
                    "longitude": None,  # Will be populated by geocoding service
                }
            }
            warehouses.append(warehouse)
        
        return warehouses
        
    except Exception as e:
        print(f"Error fetching from Airtable: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch warehouses from Airtable: {str(e)}")

async def get_warehouse_by_id(warehouse_id: str) -> Dict[str, Any]:
    """Get a specific warehouse by ID"""
    try:
        record = table.get(warehouse_id)
        if not record:
            raise HTTPException(status_code=404, detail="Warehouse not found")
        
        warehouse = {
            "id": record["id"],
            "properties": {
                "name": record["fields"].get("Name", ""),
                "city": record["fields"].get("City", ""),
                "state": record["fields"].get("State", ""),
                "zip": record["fields"].get("Zip", ""),
                "full_address": record["fields"].get("Full Address", ""),
                "status": record["fields"].get("Status", ""),
                "tier": record["fields"].get("Tier", ""),
                "contact_1": record["fields"].get("Contact 1", ""),
                "email_1": record["fields"].get("Email 1", ""),
                "office_phone": record["fields"].get("Office #", ""),
                "cell_phone": record["fields"].get("Cell # 1", ""),
                "contact_2": record["fields"].get("Contact 2", ""),
                "cell_phone_2": record["fields"].get("Cell # 2", ""),
                "email_2": record["fields"].get("Email 2", ""),
                "email_3": record["fields"].get("Email 3", ""),
                "hours": record["fields"].get("Hours", ""),
                "hazmat": record["fields"].get("Hazmat", ""),
                "temp_control": record["fields"].get("Temp Control", ""),
                "food_grade": record["fields"].get("Food Grade", ""),
                "paper_rolls": record["fields"].get("Paper Rolls", False),
                "services": record["fields"].get("Services", []),
                "website": record["fields"].get("Website", ""),
                "notes_pricing": record["fields"].get("Notes / Pricing", ""),
                "insurance": record["fields"].get("Insurance", ""),
                "latitude": None,
                "longitude": None,
            }
        }
        
        return warehouse
        
    except Exception as e:
        print(f"Error fetching warehouse {warehouse_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch warehouse: {str(e)}")

async def search_warehouses_by_location(city: str = None, state: str = None, zip_code: str = None) -> List[Dict[str, Any]]:
    """Search warehouses by location criteria"""
    try:
        # Build filter formula
        filters = []
        if city:
            filters.append(f"{{City}} = '{city}'")
        if state:
            filters.append(f"{{State}} = '{state}'")
        if zip_code:
            filters.append(f"{{Zip}} = '{zip_code}'")
        
        if not filters:
            return await fetch_warehouses_from_airtable()
        
        # Create filter formula
        filter_formula = "AND(" + ", ".join(filters) + ")"
        
        # Get filtered records
        records = table.all(formula=filter_formula)
        
        # Transform records
        warehouses = []
        for record in records:
            warehouse = {
                "id": record["id"],
                "properties": {
                    "name": record["fields"].get("Name", ""),
                    "city": record["fields"].get("City", ""),
                    "state": record["fields"].get("State", ""),
                    "zip": record["fields"].get("Zip", ""),
                    "full_address": record["fields"].get("Full Address", ""),
                    "status": record["fields"].get("Status", ""),
                    "tier": record["fields"].get("Tier", ""),
                    "contact_1": record["fields"].get("Contact 1", ""),
                    "email_1": record["fields"].get("Email 1", ""),
                    "office_phone": record["fields"].get("Office #", ""),
                    "cell_phone": record["fields"].get("Cell # 1", ""),
                    "contact_2": record["fields"].get("Contact 2", ""),
                    "cell_phone_2": record["fields"].get("Cell # 2", ""),
                    "email_2": record["fields"].get("Email 2", ""),
                    "email_3": record["fields"].get("Email 3", ""),
                    "hours": record["fields"].get("Hours", ""),
                    "hazmat": record["fields"].get("Hazmat", ""),
                    "temp_control": record["fields"].get("Temp Control", ""),
                    "food_grade": record["fields"].get("Food Grade", ""),
                    "paper_rolls": record["fields"].get("Paper Rolls", False),
                    "services": record["fields"].get("Services", []),
                    "website": record["fields"].get("Website", ""),
                    "notes_pricing": record["fields"].get("Notes / Pricing", ""),
                    "insurance": record["fields"].get("Insurance", ""),
                    "latitude": None,
                    "longitude": None,
                }
            }
            warehouses.append(warehouse)
        
        return warehouses
        
    except Exception as e:
        print(f"Error searching warehouses: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to search warehouses: {str(e)}")

async def get_warehouse_stats() -> Dict[str, Any]:
    """Get warehouse statistics summary"""
    try:
        warehouses = await fetch_warehouses_from_airtable()
        
        if not warehouses:
            return {"total": 0, "by_status": {}, "by_tier": {}, "by_state": {}}
        
        stats = {
            "total": len(warehouses),
            "by_status": {},
            "by_tier": {},
            "by_state": {}
        }
        
        for warehouse in warehouses:
            props = warehouse.get("properties", {})
            
            # Count by status
            status = props.get("status", "Unknown")
            stats["by_status"][status] = stats["by_status"].get(status, 0) + 1
            
            # Count by tier
            tier = props.get("tier", "Unknown")
            stats["by_tier"][tier] = stats["by_tier"].get(tier, 0) + 1
            
            # Count by state
            state = props.get("state", "Unknown")
            stats["by_state"][state] = stats["by_state"].get(state, 0) + 1
        
        return stats
        
    except Exception as e:
        print(f"Error getting warehouse stats: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get warehouse stats: {str(e)}") 