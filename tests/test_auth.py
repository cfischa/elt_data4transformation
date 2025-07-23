#!/usr/bin/env python3
"""
Test authentication for DESTATIS connector.
"""

import asyncio
import os
from connectors.destatis_connector import DestatisConnector, DestatisConfig

async def test_auth():
    """Test authentication with DESTATIS API."""
    
    # Try to get token from environment
    token = os.getenv("DESTATIS_TOKEN")
    if not token:
        print("❌ No DESTATIS_TOKEN environment variable found")
        print("Please set your token: $env:DESTATIS_TOKEN = 'your_token_here'")
        return
    
    print(f"🔑 Using token: {token[:8]}...")
    
    # Create connector with token
    config = DestatisConfig(api_token=token)
    
    try:
        async with DestatisConnector(config) as connector:
            print("🔍 Testing login check endpoint...")
            
            # Test the login check endpoint
            response = await connector._make_request("helloworld/logincheck")
            data = response.json()
            
            print("✅ Authentication successful!")
            print(f"Response: {data}")
            
            # Test getting metadata for a small table
            print("\n🔍 Testing metadata endpoint...")
            table_info = await connector.get_table_info("12411-0001")
            print(f"✅ Table info retrieved: {table_info.name} - {table_info.description[:100]}...")
            
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        print("Please check your token is valid and has the right permissions")

if __name__ == "__main__":
    asyncio.run(test_auth())
