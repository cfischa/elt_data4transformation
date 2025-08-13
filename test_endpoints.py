#!/usr/bin/env python3
"""
Test script to check available Destatis API endpoints.
"""

import asyncio
import httpx
import os
from dotenv import load_dotenv

load_dotenv()

async def test_endpoints():
    """Test various Destatis API endpoints to see which ones exist."""
    
    base_url = "https://www-genesis.destatis.de/genesisWS/rest/2020"
    
    # Potential endpoints to test
    endpoints_to_test = [
        "helloworld/logincheck",
        "catalogue/cubes", 
        "catalogue/tables",
        "find/table",
        "find/cube",
        "metadata/table",
        "metadata/cube",
        "data/table",
        "data/cubefile",
        "data/tablefile"
    ]
    
    # Authentication data
    auth_data = {
        "username": os.getenv("DESTATIS_USER", ""),
        "password": os.getenv("DESTATIS_PASS", ""),
        "language": "en"
    }
    
    print(f"🔍 Testing Destatis API endpoints at {base_url}")
    print(f"Using auth: username='{auth_data['username']}', password={'*' * len(auth_data['password'])}")
    print("=" * 60)
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        for endpoint in endpoints_to_test:
            url = f"{base_url}/{endpoint}"
            try:
                # Test with minimal data
                test_data = auth_data.copy()
                if "find" in endpoint:
                    test_data["term"] = "*"
                elif "metadata" in endpoint or "data" in endpoint:
                    test_data["name"] = "12411*"  # Common table pattern
                
                response = await client.post(url, data=test_data)
                
                status = response.status_code
                content_length = len(response.content)
                
                if status == 200:
                    print(f"✅ {endpoint:<25} - HTTP {status} ({content_length} bytes)")
                elif status == 404:
                    print(f"❌ {endpoint:<25} - HTTP {status} (Not Found)")
                elif status == 401:
                    print(f"🔐 {endpoint:<25} - HTTP {status} (Auth Required)")
                elif status == 400:
                    print(f"⚠️ {endpoint:<25} - HTTP {status} (Bad Request - but endpoint exists)")
                else:
                    print(f"❓ {endpoint:<25} - HTTP {status}")
                    
            except Exception as e:
                print(f"💥 {endpoint:<25} - Error: {str(e)[:50]}...")

if __name__ == "__main__":
    asyncio.run(test_endpoints())
