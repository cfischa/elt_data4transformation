#!/usr/bin/env python3
"""
Test guest access capabilities with DESTATIS API.
"""

import asyncio
from connectors.destatis_connector import DestatisConnector, DestatisConfig

async def test_guest_access():
    """Test what we can access with guest permissions."""
    
    token = "17a1d34b0e3b44c4bfe456c872ef8fc5"
    print(f"🔑 Using token: {token}")
    
    config = DestatisConfig(api_token=token)
    
    try:
        async with DestatisConnector(config) as connector:
            print("✅ Testing guest access capabilities...")
            
            # Try to get available tables (might work with guest access)
            try:
                print("\n🔍 Testing find/table endpoint...")
                response = await connector._make_request("find/table", {"term": "population"})
                data = response.json()
                print(f"✅ Find tables works! Found {len(data.get('List', []))} results")
                if data.get('List'):
                    first_table = data['List'][0]
                    print(f"Example table: {first_table.get('Code')} - {first_table.get('Content', '')[:100]}")
            except Exception as e:
                print(f"❌ Find tables failed: {e}")
            
            # Try a specific public table that might be accessible
            try:
                print("\n🔍 Testing public data access...")
                response = await connector._make_request("data/table", {
                    "name": "12411-0001",
                    "area": "all",
                    "format": "json",
                    "compress": "false"
                })
                data = response.json()
                print(f"✅ Data access works! Response keys: {list(data.keys())}")
            except Exception as e:
                print(f"❌ Data access failed: {e}")
                
            # Check what endpoints might be available
            print("\n🔍 Available endpoints to try:")
            endpoints_to_try = [
                "catalogue/tables",
                "catalogue/cubes", 
                "metadata/variable",
                "data/timeseries"
            ]
            
            for endpoint in endpoints_to_try:
                try:
                    response = await connector._make_request(endpoint, {})
                    print(f"✅ {endpoint}: {response.status_code}")
                except Exception as e:
                    print(f"❌ {endpoint}: {str(e)[:100]}")
                    
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_guest_access())
