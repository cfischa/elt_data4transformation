#!/usr/bin/env python3
"""
Test Destatis metadata fetch with working token authentication and longer timeout
"""

import asyncio
import httpx
import os
from dotenv import load_dotenv

load_dotenv()

async def test_metadata_with_token():
    """Test metadata fetch with working token and longer timeout."""
    
    base_url = "https://www-genesis.destatis.de/genesisWS/rest/2020/"
    token = os.getenv('DESTATIS_API_KEY', '')
    
    print(f"🔍 Testing metadata fetch with token: {token[:10]}...")
    
    # Use longer timeout to handle slow responses
    async with httpx.AsyncClient(timeout=60.0) as client:
        
        # Test catalogue/cubes with working token and longer timeout
        print("\n📋 Testing: catalogue/cubes with token + longer timeout")
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "username": token,
            "password": ""
        }
        
        try:
            print("   Making request...")
            response = await client.post(
                base_url + "catalogue/cubes",
                headers=headers,
                data={
                    "selection": "61*",  # Economic indicators
                    "pagelength": "5",   # Small sample first
                    "language": "de",
                    "format": "JSON"
                }
            )
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                print("   ✅ SUCCESS! Metadata retrieved:")
                print(f"   Response preview: {response.text[:500]}...")
                
                # Try to parse JSON to see structure
                try:
                    import json
                    data = response.json()
                    if "Object" in data:
                        cubes = data.get("Object", [])
                        print(f"   📊 Found {len(cubes)} cubes in response")
                        if cubes:
                            first_cube = cubes[0]
                            print(f"   📋 First cube: {first_cube.get('Code', 'NO CODE')} - {first_cube.get('Content', 'NO CONTENT')[:50]}...")
                except Exception as e:
                    print(f"   JSON parsing error: {e}")
            else:
                print(f"   ❌ Error: {response.text}")
                
        except asyncio.TimeoutError:
            print("   ⏰ Request timed out - server may be slow")
        except Exception as e:
            print(f"   ❌ Error: {e}")

if __name__ == "__main__":
    print("🚀 Testing metadata fetch with working authentication...")
    print("=" * 60)
    asyncio.run(test_metadata_with_token())
    print("=" * 60)
    print("🎯 Test complete!")
