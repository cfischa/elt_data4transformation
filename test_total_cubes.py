"""
Simple test to check the total number of cubes using the working DAG pattern
"""
import asyncio
import sys
import os
sys.path.append('.')

async def test_total_cubes():
    """Test with increasing page lengths to find the total number of cubes."""
    from connectors.destatis_connector import DestatisConnector
    
    print("🔍 Testing total number of cubes available from Destatis API...")
    
    # Test with progressively larger page lengths
    test_lengths = [1000, 2000, 5000, 10000, 20000]
    
    async with DestatisConnector() as connector:
        for pagelength in test_lengths:
            try:
                print(f"\n📊 Testing with pagelength={pagelength}...")
                cubes = await connector.get_available_cubes(
                    selection="*",  # Get all cubes
                    pagelength=pagelength
                )
                
                cube_count = len(cubes)
                print(f"✅ Retrieved {cube_count} cubes")
                
                # If we got fewer cubes than the page length, we have them all
                if cube_count < pagelength:
                    print(f"🎯 TOTAL CUBES AVAILABLE: {cube_count}")
                    print(f"   (This is less than pagelength={pagelength}, so we have them all)")
                    
                    # Show some examples
                    if cube_count > 0:
                        print(f"\n📋 First 5 cube examples:")
                        for i, cube in enumerate(cubes[:5]):
                            print(f"   {i+1}. {cube.code}: {cube.content[:100]}...")
                    
                    break
                else:
                    print(f"⚠️  Got exactly {cube_count} cubes - there might be more...")
                    
            except Exception as e:
                print(f"❌ Error with pagelength={pagelength}: {e}")
                import traceback
                traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_total_cubes())
