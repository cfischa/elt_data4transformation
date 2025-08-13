#!/usr/bin/env python3
"""
Test the catalogue/cubes endpoint specifically
"""

import asyncio
import httpx
import os
from dotenv import load_dotenv

load_dotenv()

async def test_catalogue_cubes():
    """Test the catalogue/cubes endpoint with different parameters."""
    
    base_url = "https://www-genesis.destatis.de/genesisWS/rest/2020/"
    token = os.getenv('DESTATIS_PASS', '')
    
    print(f"🔍 Testing catalogue/cubes endpoint with token: {token[:10]}...")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        
        # Test 1: With authentication in form data (like logincheck worked)
        print("\n1️⃣ Testing: catalogue/cubes with auth in form data")
        data1 = {
            "username": token,
            "password": "",
            "selection": "61*",  # As per your API docs
            "pagelength": "10",
            "language": "de",
            "format": "JSON"
        }
        
        try:
            response = await client.post(
                base_url + "catalogue/cubes",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=data1
            )
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:300]}")
        except Exception as e:
            print(f"   Error: {e}")
        
        # Test 2: Try without authentication (anonymous)
        print("\n2️⃣ Testing: catalogue/cubes anonymous")
        data2 = {
            "selection": "61*",
            "pagelength": "10", 
            "language": "de",
            "format": "JSON"
        }
        
        try:
            response = await client.post(
                base_url + "catalogue/cubes",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=data2
            )
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:300]}")
        except Exception as e:
            print(f"   Error: {e}")
            
        # Test 3: Try with different selection pattern
        print("\n3️⃣ Testing: catalogue/cubes with * pattern")
        data3 = {
            "username": token,
            "password": "",
            "selection": "*",
            "pagelength": "5",
            "language": "de",
            "format": "JSON"
        }
        
        try:
            response = await client.post(
                base_url + "catalogue/cubes",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=data3
            )
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:300]}")
        except Exception as e:
            print(f"   Error: {e}")

        # Test 4: Try to see if endpoint exists at all
        print("\n4️⃣ Testing: Basic GET to see if endpoint exists")
        try:
            response = await client.get(base_url + "catalogue/cubes")
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:300]}")
        except Exception as e:
            print(f"   Error: {e}")
            
        # Test 5: Try a known working endpoint for comparison  
        print("\n5️⃣ Testing: helloworld/logincheck for comparison")
        data5 = {
            "username": token,
            "password": "",
            "language": "de"
        }
        
        try:
            response = await client.post(
                base_url + "helloworld/logincheck",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=data5
            )
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:300]}")
        except Exception as e:
            print(f"   Error: {e}")

if __name__ == "__main__":
    print("🚀 Testing catalogue/cubes endpoint...")
    print("=" * 60)
    asyncio.run(test_catalogue_cubes())
    print("=" * 60)
    print("🎯 Test complete!")
