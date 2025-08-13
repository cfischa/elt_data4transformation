"""
Test script to check the total number of cubes available from Destatis API
"""
import asyncio
import sys
import os
sys.path.append('.')

from connectors.destatis_connector import DestatisConnector

async def test_cube_count():
    """Test different pagelength values to see how many cubes exist."""
    print("Testing cube counts with different page lengths...")
    
    # Test with different page lengths
    test_lengths = [1000, 2000, 5000, 10000, 50000]
    
    async with DestatisConnector() as connector:
        for length in test_lengths:
            try:
                print(f"\nTesting pagelength={length}...")
                cubes = await connector.get_available_cubes(pagelength=length)
                print(f"Retrieved {len(cubes)} cubes with pagelength={length}")
                
                if len(cubes) < length:
                    print(f"✅ Found all available cubes: {len(cubes)} total")
                    break
            except Exception as e:
                print(f"❌ Error with pagelength={length}: {e}")

if __name__ == "__main__":
    asyncio.run(test_cube_count())
