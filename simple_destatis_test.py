#!/usr/bin/env python3
"""
Simple test for Destatis connector import and basic functionality.
"""

import asyncio
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

async def simple_test():
    """Simple test to check if the connector works."""
    try:
        print("🔍 Testing basic connector functionality...")
        
        # Import the connector
        from connectors.destatis_connector import DestatisConnector
        print("✅ Successfully imported DestatisConnector")
        
        # Create connector instance
        connector = DestatisConnector()
        print("✅ Successfully created connector instance")
        
        # Test async context manager
        async with connector:
            print("✅ Successfully opened connector context")
            
            # Try to call the get_available_cubes method
            print("📋 Attempting to fetch cubes metadata...")
            cubes = await connector.get_available_cubes()
            
            print(f"✅ Successfully fetched {len(cubes)} cubes")
            
            if cubes:
                print(f"📊 First cube: {cubes[0].name}")
                print(f"📝 Description: {cubes[0].description[:100]}...")
                return True
            else:
                print("⚠️ No cubes returned")
                return False
                
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        print(f"Traceback:\n{traceback.format_exc()}")
        return False

def main():
    """Main function."""
    print("🚀 Starting simple Destatis test...")
    try:
        result = asyncio.run(simple_test())
        if result:
            print("✅ Test completed successfully!")
        else:
            print("❌ Test failed!")
        return result
    except Exception as e:
        print(f"❌ Async error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
