"""
Quick test to check total cube count with higher pagelength
"""
import asyncio
import sys
import os
sys.path.append('.')

async def test_cube_count():
    """Test with a high pagelength to see total cubes available."""
    from connectors.destatis_connector import DestatisConnector
    
    print("🔍 Testing total cube count with pagelength=50000...")
    
    async with DestatisConnector() as connector:
        try:
            cubes = await connector.get_available_cubes(pagelength=50000)
            cube_count = len(cubes)
            
            print(f"✅ Total cubes retrieved: {cube_count}")
            
            if cube_count < 50000:
                print(f"🎯 CONFIRMED: Total available cubes = {cube_count}")
                print("   (This is less than our pagelength limit, so we have them all)")
            else:
                print(f"⚠️  Got exactly {cube_count} - might need even higher pagelength")
            
            # Show some examples
            if cube_count > 0:
                print(f"\n📋 First 3 cube examples:")
                for i, cube in enumerate(cubes[:3]):
                    print(f"   {i+1}. {cube.code}: {cube.content[:80]}...")
                
                print(f"\n📋 Last 3 cube examples:")
                for i, cube in enumerate(cubes[-3:]):
                    print(f"   {cube_count-2+i}. {cube.code}: {cube.content[:80]}...")
                    
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_cube_count())
