#!/usr/bin/env python3
"""
Debug the connector authentication values
"""

import asyncio
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

async def debug_connector_auth():
    """Debug what authentication values the connector is using."""
    
    from connectors.destatis_connector import DestatisConnector
    
    # Create connector and check its configuration
    connector = DestatisConnector()
    
    print("🔍 Debugging connector authentication:")
    print(f"   API Token: {connector.config.api_token}")
    print(f"   Username: {connector.config.username}")
    print(f"   Password: {connector.config.password}")
    
    # Check what headers would be generated
    headers = connector._prepare_request_headers()
    print(f"\n📋 Generated headers:")
    for key, value in headers.items():
        if key in ['username', 'password']:
            print(f"   {key}: {value}")
        else:
            print(f"   {key}: {value}")
    
    return headers

if __name__ == "__main__":
    print("🚀 Debugging connector authentication...")
    print("=" * 60)
    result = asyncio.run(debug_connector_auth())
    print("=" * 60)
    print("🎯 Debug complete!")
