#!/usr/bin/env python3
"""
Test script for local webhook testing
"""
import requests
import json

# Local webhook URL
LOCAL_WEBHOOK_URL = "http://localhost:8000/webhook"

# Sample warehouse data (same as your Airtable script)
sample_warehouse_data = {
    "Warehouse Name": "Test Warehouse Local",
    "Last contact date": "2024-01-15",
    "City": "New York",
    "State": "NY",
    "ZIP": "10001",
    "Full Address": "123 Test St, New York, NY 10001",
    "Contact Name": "John Doe",
    "Office Phone Number": "(555) 123-4567",
    "Cell Phone": "(555) 987-6543",
    "Contact Email": "john@testwarehouse.com",
    "Status": "Active",
    "WHN User ": "testuser",
    "Cleaned Data": "Yes",
    "Onboarding Call Scheduled": "2024-01-20",
    "Onboarded": "No",
    "Email 3": "admin@testwarehouse.com",
    "Contact 2 Email": "manager@testwarehouse.com",
    "Tier": "Premium",
    "Hours of Operation": "9 AM - 5 PM",
    "Weekends": "No",
    "Services Offered": "Storage, Shipping",
    "Bonded": "Yes",
    "Food Grade": "Yes",
    "Hazmat": "No",
    "Paper Clamps": "Yes",
    "Warehouse Temp Controlled": "Yes",
    "Disposal": "Available",
    "Specialized Equipement": "Forklifts",
    "Dumpster Size": "Large",
    "Willing to Order Dumpster": "Yes",
    "Notes": "Test warehouse for local webhook testing",
    "Parking Spots": "Yes",
    "# of Parking Spots": "50",
    "Parking Notes": "Free parking available",
    "High Cargo Value": "Yes",
    "Supplier Approved": "Yes",
    "Contact 2 Name": "Jane Smith",
    "Contact 2 Phone Number": "(555) 456-7890",
    "Website": "https://testwarehouse.com",
    "Insurance": "Yes",
    "Insurance via link": "https://insurance.com",
    "Requests": "None",
    "Personnel Notes": "Friendly staff",
    "Dashboard": "Active"
}

def test_local_webhook():
    """Test the local webhook endpoint"""
    try:
        print("🧪 Testing LOCAL webhook endpoint...")
        print(f"📡 Sending to: {LOCAL_WEBHOOK_URL}")
        print(f"📦 Data: {sample_warehouse_data['Warehouse Name']}")
        
        # Send POST request
        response = requests.post(
            LOCAL_WEBHOOK_URL,
            headers={"Content-Type": "application/json"},
            json=sample_warehouse_data,
            timeout=10
        )
        
        print(f"📊 Status Code: {response.status_code}")
        print(f"📄 Response: {response.text}")
        
        if response.status_code == 200:
            print("✅ Local webhook test successful!")
            return True
        else:
            print("❌ Local webhook test failed!")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ Connection error: Make sure your app is running on localhost:8080")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Starting LOCAL webhook test...")
    print("💡 Make sure your app is running with: python -m uvicorn main:app --host 0.0.0.0 --port 8080 --reload")
    print()
    
    success = test_local_webhook()
    
    if success:
        print("🎉 Local webhook is working correctly!")
        print("✅ You can now update your Airtable script with the local URL")
    else:
        print("💥 Local webhook test failed. Check if your app is running.")
