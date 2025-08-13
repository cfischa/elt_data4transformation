#!/usr/bin/env python3
"""
Test token + empty password authentication
"""

import asyncio
import httpx
import os
from dotenv import load_dotenv

load_dotenv()

async def test_token_auth():
    """Test token + empty password authentication."""
    
    base_url = "https://www-genesis.destatis.de/genesisWS/rest/2020/"
    token = os.getenv('DESTATIS_API_KEY', '')
    
    print(f"🔍 Testing token + empty password with: {token[:10]}...")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        
        # Test 1: Token as username + empty password in headers
        print("\n1️⃣ Testing: token as username + empty password in headers")
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
            
            # Check authentication result
            if "GAST" in response.text:
                print("   ⚠️  Still authenticated as GAST")
            elif "erfolgreich" in response.text:
                print("   ✅ Authentication successful")
            else:
                print("   ❌ Authentication failed")
                
        except Exception as e:
            print(f"   Error: {e}")
        
        # Test 2: Same method with catalogue/cubes
        print("\n2️⃣ Testing: catalogue/cubes with token + empty password")
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
            
            if response.status_code == 200:
                print("   ✅ Catalogue access successful!")
            elif "nicht berechtigt" in response.text:
                print("   ❌ Not authorized for this service")
            else:
                print("   ❌ Other error")
                
        except Exception as e:
            print(f"   Error: {e}")

        # Test 3: Try a basic table endpoint
        print("\n3️⃣ Testing: data/tablefile with token + empty password")
        headers3 = {
            "Content-Type": "application/x-www-form-urlencoded", 
            "username": token,
            "password": ""
        }
        
        try:
            response = await client.post(
                base_url + "data/tablefile",
                headers=headers3,
                data={
                    "name": "61111-0001",
                    "format": "csv",
                    "language": "de"
                }
            )
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            
        except Exception as e:
            print(f"   Error: {e}")

if __name__ == "__main__":
    print("🚀 Testing token + empty password authentication...")
    print("=" * 60)
    asyncio.run(test_token_auth())
    print("=" * 60)
    print("🎯 Test complete!")
