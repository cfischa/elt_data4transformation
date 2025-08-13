#!/usr/bin/env python3
"""
Test authentication with credentials in headers as per API documentation
"""

import asyncio
import httpx
import os
from dotenv import load_dotenv

load_dotenv()

async def test_header_auth():
    """Test authentication with credentials in headers."""
    
    base_url = "https://www-genesis.destatis.de/genesisWS/rest/2020/"
    token = os.getenv('DESTATIS_API_KEY', '')
    
    print(f"🔍 Testing authentication in headers with token: {token[:10]}...")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        
        # Test 1: logincheck with credentials in headers
        print("\n1️⃣ Testing: logincheck with credentials in headers")
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
            
            # Check if we're authenticated as something other than GAST
            if "GAST" in response.text:
                print("   ⚠️  Still authenticated as GAST")
            else:
                print("   ✅ Authenticated with credentials")
                
        except Exception as e:
            print(f"   Error: {e}")
        
        # Test 2: Try catalogue/cubes with credentials in headers
        print("\n2️⃣ Testing: catalogue/cubes with credentials in headers")
        headers2 = {
            "Content-Type": "application/x-www-form-urlencoded", 
            "username": token,
            "password": ""
        }
        
        try:
            response = await client.post(
                base_url + "catalogue/cubes",
                headers=headers2,
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
    print("🚀 Testing authentication in headers...")
    print("=" * 60)
    asyncio.run(test_header_auth())
    print("=" * 60)
    print("🎯 Test complete!")
