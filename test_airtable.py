import os
import asyncio
from dotenv import load_dotenv
from pyairtable import Api, Base, Table

load_dotenv()

# Test Airtable Connection
async def test_airtable_connection():
    print("🔍 Testing Airtable Connection...")
    print("=" * 50)
    
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("AIRTABLE_BASE_ID")
    
    if not api_key:
        print("❌ AIRTABLE_API_KEY not found in environment variables")
        print("Please create a .env file with your API key")
        print(f"Current working directory: {os.getcwd()}")
        print(f"Looking for .env file in: {os.path.join(os.getcwd(), '.env')}")
        return False
    
    print(f"✅ API Key found: {api_key[:10]}...{api_key[-4:]}")
    print(f"✅ Base ID: {base_id}")
    
    try:
        # Initialize Airtable
        print("\n🔄 Initializing Airtable connection...")
        airtable = Api(api_key)
        base = Base(api_key, base_id)
        table = Table(api_key, base_id, "Warehouses")
        
        print("✅ Airtable connection successful!")
        
        # Test fetching records
        print("\n📊 Fetching warehouse records...")
        records = table.all()
        
        print(f"✅ Found {len(records)} warehouse records")
        
        if records:
            print("\n📋 Sample Record Structure:")
            print("-" * 30)
            sample_record = records[0]
            print(f"Record ID: {sample_record['id']}")
            print("Fields:")
            for field_name, field_value in sample_record['fields'].items():
                print(f"  {field_name}: {field_value}")
        
        # Test specific fields
        print("\n🔍 Testing specific field access...")
        if records:
            first_record = records[0]
            fields = first_record['fields']
            
            print(f"Name: {fields.get('Name', 'N/A')}")
            print(f"City: {fields.get('City', 'N/A')}")
            print(f"State: {fields.get('State', 'N/A')}")
            print(f"Zip: {fields.get('Zip', 'N/A')}")
            print(f"Status: {fields.get('Status', 'N/A')}")
            print(f"Tier: {fields.get('Tier', 'N/A')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error connecting to Airtable: {e}")
        return False

async def test_search_functionality():
    print("\n🔍 Testing Search Functionality...")
    print("=" * 50)
    
    try:
        api_key = os.getenv("AIRTABLE_API_KEY")
        base_id = os.getenv("AIRTABLE_BASE_ID", "appqsjdQHyqZ5OMrx")
        table = Table(api_key, base_id, "Warehouses")
        
        # Test filtering by status
        print("🔍 Searching for Active warehouses...")
        active_warehouses = table.all(formula="{Status} = 'Active'")
        print(f"✅ Found {len(active_warehouses)} active warehouses")
        
        # Test filtering by state
        print("🔍 Searching for CA warehouses...")
        ca_warehouses = table.all(formula="{State} = 'CA'")
        print(f"✅ Found {len(ca_warehouses)} CA warehouses")
        
        # Test filtering by tier
        print("🔍 Searching for Gold tier warehouses...")
        gold_warehouses = table.all(formula="{Tier} = 'Gold'")
        print(f"✅ Found {len(gold_warehouses)} Gold tier warehouses")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing search: {e}")
        return False

# Main test function
async def main():
    print("🚀 Airtable Connection Test")
    print("=" * 50)
    
    # Test basic connection
    connection_success = await test_airtable_connection()
    
    if connection_success:
        # Test search functionality
        await test_search_functionality()
        
        print("\n🎉 All tests completed successfully!")
        print("Your Airtable integration is ready!")
    else:
        print("\n💥 Connection test failed!")
        print("Please check your API key and try again.")

if __name__ == "__main__":
    asyncio.run(main())
