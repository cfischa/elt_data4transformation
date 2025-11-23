import asyncio
import logging
import sys
from datetime import datetime
from typing import Any, Dict, List

# Configure simple logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger("LiveTest")

# Import connectors
try:
    from connectors.soep_connector import SOEPConnector
    from connectors.eurostat_connector import EurostatConnector
except ImportError:
    logger.error("Could not import connectors. Run this script as a module: python -m scripts.test_live_connectors")
    sys.exit(1)


async def test_soep_live():
    """Test SOEP connector against real API."""
    logger.info("--- Testing SOEP Connector (Live) ---")
    
    try:
        async with SOEPConnector() as connector:
            # 1. Test Metadata Fetch
            logger.info("Fetching metadata (page 1)...")
            metadata = await connector.fetch_metadata(max_pages=1)
            
            if not metadata:
                logger.error("❌ SOEP: No metadata returned")
                return False
            
            logger.info(f"✅ SOEP: Fetched {len(metadata)} indicators")
            
            # 2. Test Data Fetch (using first indicator)
            test_indicator = metadata[0]
            slug = test_indicator["slug"]
            logger.info(f"Fetching data for indicator: {slug} ({test_indicator.get('title', 'No Title')})")
            
            observation_count = 0
            async for result in connector.fetch_data(datasets=[slug]):
                observations = result.get("observations", [])
                observation_count += len(observations)
                if observations:
                    logger.info(f"  Sample observation: {observations[0]}")
            
            if observation_count > 0:
                logger.info(f"✅ SOEP: Successfully fetched {observation_count} observations")
                return True
            else:
                logger.warning("⚠️ SOEP: Fetched 0 observations (might be empty dataset)")
                return True  # Still a success in terms of connectivity

    except Exception as e:
        logger.error(f"❌ SOEP: Failed with error: {e}")
        return False


async def test_eurostat_live():
    """Test Eurostat connector against real API."""
    logger.info("\n--- Testing Eurostat Connector (Live) ---")
    
    try:
        async with EurostatConnector() as connector:
            # 1. Test Dataset Listing (TOC)
            logger.info("Listing datasets (first 5)...")
            datasets = await connector.get_available_datasets(page_size=5)
            
            if not datasets:
                logger.error("❌ Eurostat: No datasets returned")
                return False
                
            logger.info(f"✅ Eurostat: Found {len(datasets)} datasets")
            for ds in datasets:
                logger.info(f"  - {ds['code']}: {ds['title']}")
            
            # 2. Test Data Fetch
            # Use a reliable dataset code if available, otherwise pick the first one
            # 'nama_10_gdp' is very standard (GDP and main components)
            target_code = "nama_10_gdp" 
            
            # Check if our target is in the list, otherwise just use the first one
            if not any(d['code'] == target_code for d in datasets):
                target_code = datasets[0]['code']
            
            logger.info(f"Fetching dataset: {target_code}")
            
            # Fetch with a small filter to avoid huge download if possible
            # We'll just fetch the dataset info first to verify it exists
            # then fetch data.
            
            # For 'nama_10_gdp', let's try to filter to recent years to keep it light
            # But for a generic test, we rely on the connector's default behavior or simple params
            # The fetch_dataset method downloads the whole JSON-stat response for the ID.
            
            # Let's try fetching with a time range if it's a known dataset, otherwise standard
            time_filter = None
            current_year = datetime.now().year
            if target_code == "nama_10_gdp":
                time_filter = str(current_year - 2) # Recent data
            
            data = await connector.fetch_dataset(
                target_code, 
                time_range=time_filter,
                language="en"
            )
            
            records = data.get("records", [])
            logger.info(f"✅ Eurostat: Fetched {len(records)} records for {target_code}")
            
            if records:
                logger.info(f"  Sample record: {records[0]}")
            
            return True

    except Exception as e:
        logger.error(f"❌ Eurostat: Failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Run all live tests."""
    print("Starting Live Connector Tests...")
    print("WARNING: This connects to real external APIs.\n")
    
    soep_result = await test_soep_live()
    eurostat_result = await test_eurostat_live()
    
    print("\n--- Summary ---")
    print(f"SOEP:     {'✅ PASS' if soep_result else '❌ FAIL'}")
    print(f"Eurostat: {'✅ PASS' if eurostat_result else '❌ FAIL'}")
    
    if soep_result and eurostat_result:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

