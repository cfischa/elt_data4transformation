#!/usr/bin/env python3
"""
Test different authentication methods for Destatis API
"""

import asyncio
import httpx
from pathlib import Path
import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def test_auth_methods():
    """Test different authentication methods based on the API docs."""
    
    base_url = "https://www-genesis.destatis.de/genesisWS/rest/2020/"
    token = os.getenv('DESTATIS_PASS', '')
    
    print(f"🔍 Testing authentication with token: {token[:10]}...")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        
        # Test 1: Username/password in headers (as per your docs)
        print("\n1️⃣ Testing: username/password in headers")
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
            print(f"   Response: {response.text[:200]}")
        except Exception as e:
            print(f"   Error: {e}")
        
        # Test 2: Username/password in form data
        print("\n2️⃣ Testing: username/password in form data")
        headers2 = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data2 = {
            "username": token,
            "password": "",
            "language": "de"
        }
        
        try:
            response = await client.post(
                base_url + "helloworld/logincheck",
                headers=headers2,
                data=data2
            )
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
        except Exception as e:
            print(f"   Error: {e}")
            
        # Test 3: Basic Auth
        print("\n3️⃣ Testing: Basic Authorization header")
        headers3 = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {token}"
        }
        
        try:
            response = await client.post(
                base_url + "helloworld/logincheck",
                headers=headers3,
                data={"language": "de"}
            )
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
        except Exception as e:
            print(f"   Error: {e}")
            
        # Test 4: Bearer token
        print("\n4️⃣ Testing: Bearer token")
        headers4 = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Bearer {token}"
        }
        
        try:
            response = await client.post(
                base_url + "helloworld/logincheck",
                headers=headers4,
                data={"language": "de"}
            )
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
        except Exception as e:
            print(f"   Error: {e}")

        # Test 5: No auth (anonymous)
        print("\n5️⃣ Testing: No authentication (anonymous)")
        headers5 = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        try:
            response = await client.post(
                base_url + "helloworld/logincheck",
                headers=headers5,
                data={"language": "de"}
            )
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
        except Exception as e:
            print(f"   Error: {e}")

if __name__ == "__main__":
    print("🚀 Testing Destatis authentication methods...")
    print("=" * 60)
    asyncio.run(test_auth_methods())
    print("=" * 60)
    print("🎯 Test complete!")
