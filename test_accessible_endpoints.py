#!/usr/bin/env python3
"""
Test what endpoints are accessible with our current token
"""

import asyncio
import httpx
import os
from dotenv import load_dotenv

load_dotenv()

async def test_accessible_endpoints():
    """Test what endpoints we can actually access."""
    
    base_url = "https://www-genesis.destatis.de/genesisWS/rest/2020/"
    token = os.getenv('DESTATIS_PASS', '')
    
    print(f"🔍 Testing accessible endpoints with token: {token[:10]}...")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        
        endpoints_to_test = [
            # Basic endpoints
            ("helloworld/logincheck", {"language": "de"}),
            ("helloworld/whoami", {"language": "de"}),
            
            # Catalogue endpoints (might need permissions)
            ("catalogue/tables", {"selection": "61*", "pagelength": "5", "language": "de"}),
            ("catalogue/variables", {"selection": "*", "pagelength": "5", "language": "de"}),
            ("catalogue/values", {"selection": "*", "pagelength": "5", "language": "de"}),
            
            # Find endpoints (alternative discovery)
            ("find/find", {"term": "bevolkerung", "category": "tables", "pagelength": "5", "language": "de"}),
            ("find/table", {"selection": "61*", "pagelength": "5", "language": "de"}),
            
            # Data endpoints (might work for specific known tables)
            ("data/tablefile", {"name": "61111-0001", "format": "csv", "language": "de"}),
        ]
        
        for endpoint, base_data in endpoints_to_test:
            print(f"\n🔍 Testing: {endpoint}")
            
            # Add authentication to data
            data = base_data.copy()
            data.update({
                "username": token,
                "password": ""
            })
            
            try:
                response = await client.post(
                    base_url + endpoint,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    data=data
                )
                print(f"   Status: {response.status_code}")
                
                if response.status_code == 200:
                    text = response.text[:300]
                    if '"Type":"ERROR"' in text:
                        print(f"   ❌ API Error: {text}")
                    else:
                        print(f"   ✅ Success: {text}")
                else:
                    print(f"   ❌ HTTP Error: {response.text[:200]}")
                    
            except Exception as e:
                print(f"   ❌ Exception: {e}")

if __name__ == "__main__":
    print("🚀 Testing accessible endpoints...")
    print("=" * 60)
    asyncio.run(test_accessible_endpoints())
    print("=" * 60)
    print("🎯 Test complete!")
