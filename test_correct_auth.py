#!/usr/bin/env python3
"""
Test the correct authentication credentials
"""

import asyncio
import httpx
import os
from dotenv import load_dotenv

load_dotenv()

async def test_correct_auth():
    """Test with the correct credentials."""
    
    base_url = "https://www-genesis.destatis.de/genesisWS/rest/2020/"
    
    # Get credentials
    token = os.getenv('DESTATIS_API_KEY', '')
    username = os.getenv('DESTATIS_USER', '')
    password = os.getenv('DESTATIS_PASS', '')
    
    print(f"🔍 Testing authentication with:")
    print(f"   Token: {token[:10]}...")
    print(f"   Username: {username}")
    print(f"   Password: {password}")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        
        # Test 1: Token + empty password in headers
        print("\n1️⃣ Testing: Token + empty password")
        headers1 = {
            "Content-Type": "application/x-www-form-urlencoded",
            "username": token,
            "password": ""
        }
        
        try:
            response = await client.post(
                base_url + "helloworld/logincheck",
                headers=headers1,
                data={"language": "de"}
            )
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text}")
            
        except Exception as e:
            print(f"   Error: {e}")
        
        # Test 2: Username + password in headers
        print("\n2️⃣ Testing: Username + password")
        headers2 = {
            "Content-Type": "application/x-www-form-urlencoded",
            "username": username,
            "password": password
        }
        
        try:
            response = await client.post(
                base_url + "helloworld/logincheck",
                headers=headers2,
                data={"language": "de"}
            )
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text}")
            
        except Exception as e:
            print(f"   Error: {e}")
            
        # Test 3: Try catalogue/cubes with token
        print("\n3️⃣ Testing: catalogue/cubes with token")
        headers3 = {
            "Content-Type": "application/x-www-form-urlencoded",
            "username": token,
            "password": ""
        }
        
        try:
            response = await client.post(
                base_url + "catalogue/cubes",
                headers=headers3,
                data={
                    "selection": "61*",
                    "pagelength": "5",
                    "language": "de",
                    "format": "JSON"
                }
            )
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:300]}")
            
        except Exception as e:
            print(f"   Error: {e}")
            
        # Test 4: Try catalogue/cubes with username+password
        print("\n4️⃣ Testing: catalogue/cubes with username+password")
        headers4 = {
            "Content-Type": "application/x-www-form-urlencoded",
            "username": username,
            "password": password
        }
        
        try:
            response = await client.post(
                base_url + "catalogue/cubes",
                headers=headers4,
                data={
                    "selection": "61*",
                    "pagelength": "5",
                    "language": "de",
                    "format": "JSON"
                }
            )
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:300]}")
            
        except Exception as e:
            print(f"   Error: {e}")

if __name__ == "__main__":
    print("🚀 Testing correct authentication credentials...")
    print("=" * 60)
    asyncio.run(test_correct_auth())
    print("=" * 60)
    print("🎯 Test complete!")
