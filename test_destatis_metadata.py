#!/usr/bin/env python3
"""
Test script for Destatis metadata fetching functionality.
Tests the new get_available_cubes() method from the refactored connector.
"""

import asyncio
import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

async def test_destatis_metadata():
    """Test the Destatis metadata fetching functionality."""
    try:
        print("🔍 Testing Destatis metadata fetch...")
        
        # Import the connector
        from connectors.destatis_connector import DestatisConnector
        
        # Test the connection and metadata fetch
        async with DestatisConnector() as connector:
            print("✅ Connector initialized successfully")
            
            # Test getting available cubes
            print("📋 Fetching available cubes from catalogue...")
            cubes = await connector.get_available_cubes()
            
            print(f"✅ Retrieved {len(cubes)} cubes from Destatis catalogue")
            
            if cubes:
                print("\n📊 Sample cubes:")
                for i, cube in enumerate(cubes[:5]):
                    print(f"  {i+1}. {cube.code}")
                    print(f"     Content: {cube.content[:100] if cube.content else 'No description'}...")
                    print(f"     State: {cube.state}")
                    print()
                
                # Test filtering for relevant cubes
                relevant_patterns = [
                    "volkswirtschaft",    # Economic indicators
                    "bevoelkerung",       # Population data
                    "arbeitsmarkt",       # Labor market
                    "preise"              # Prices/inflation
                ]
                
                relevant_cubes = []
                for cube in cubes:
                    cube_code_lower = cube.code.lower()
                    cube_content_lower = (cube.content or '').lower()
                    
                    if any(pattern in cube_code_lower or pattern in cube_content_lower 
                           for pattern in relevant_patterns):
                        relevant_cubes.append(cube)
                
                print(f"🎯 Found {len(relevant_cubes)} relevant cubes matching economic patterns")
                
                if relevant_cubes:
                    print("\n💼 Relevant cubes:")
                    for i, cube in enumerate(relevant_cubes[:3]):
                        print(f"  {i+1}. {cube.code}")
                        print(f"     Content: {cube.content[:80] if cube.content else 'No description'}...")
                        print()
                
                return {
                    "status": "success",
                    "total_cubes": len(cubes),
                    "relevant_cubes": len(relevant_cubes),
                    "sample_codes": [cube.code for cube in cubes[:3]]
                }
            else:
                print("❌ No cubes retrieved from API")
                return {"status": "failed", "error": "No cubes found"}
                
    except Exception as e:
        print(f"❌ Error during metadata fetch: {e}")
        import traceback
        print(f"Full traceback:\n{traceback.format_exc()}")
        return {"status": "failed", "error": str(e)}

async def main():
    """Main test function."""
    print("🚀 Starting Destatis metadata fetch test...")
    print("=" * 60)
    
    result = await test_destatis_metadata()
    
    print("=" * 60)
    print(f"🎯 Test Result: {result['status']}")
    
    if result['status'] == 'success':
        print(f"✅ Successfully fetched metadata for {result['total_cubes']} cubes")
        print(f"🎯 Found {result['relevant_cubes']} relevant cubes")
        print("📝 Sample cube codes:")
        for code in result['sample_codes']:
            print(f"   - {code}")
    else:
        print(f"❌ Test failed: {result.get('error', 'Unknown error')}")
    
    return result

if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(0 if result['status'] == 'success' else 1)
