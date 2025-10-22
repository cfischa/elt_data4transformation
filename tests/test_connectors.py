#!/usr/bin/env python3
"""Test script for data source connectors"""


import sys
import asyncio
from pathlib import Path
import os

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_clickhouse_connection():
    """Test ClickHouse connection"""
    try:
        import clickhouse_connect
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8124,
            username=os.getenv('CLICKHOUSE_USER', ''),
            password=os.getenv('CLICKHOUSE_PASSWORD', '')
        )
        version = client.query('SELECT version()').result_rows[0][0]
        print(f"✅ ClickHouse connection successful: {version}")
        return True
    except Exception as e:
        print(f"❌ ClickHouse connection failed: {e}")
        return False

async def test_dawum_connector():
    """Test DAWUM API connector"""
    try:
        from connectors.dawum_connector import DawumConnector
        connector = DawumConnector()
        # Just test that the connector can be instantiated and has the right methods
        assert hasattr(connector, 'fetch_polls'), "DAWUM connector missing fetch_polls method"
        print("✅ DAWUM connector initialized successfully (API call test skipped)")
        return True
    except Exception as e:
        print(f"❌ DAWUM connector failed: {e}")
        return False

async def test_destatis_connector():
    """Test Destatis API connector"""
    try:
        from connectors.destatis_connector import DestatisConnector
        async with DestatisConnector() as connector:
            # Just test that the connector can be instantiated and has the right methods
            assert hasattr(connector, 'get_table_info'), "Destatis connector missing get_table_info method"
            assert hasattr(connector, 'fetch_table'), "Destatis connector missing fetch_table method"
            print("✅ Destatis connector initialized successfully (API call test skipped)")
        return True
    except Exception as e:
        print(f"❌ Destatis connector failed: {e}")
        return False

async def test_eurostat_connector():
    """Test Eurostat API connector"""
    try:
        from connectors.eurostat_connector import EurostatConnector

        connector = EurostatConnector()
        assert hasattr(connector, 'list_datasets'), "Eurostat connector missing list_datasets method"
        assert hasattr(connector, 'fetch_dataset'), "Eurostat connector missing fetch_dataset method"
        assert hasattr(connector, 'fetch_data'), "Eurostat connector missing fetch_data method"
        print("✅ Eurostat connector initialized successfully (API call test skipped)")
        return True
    except Exception as e:
        print(f"❌ Eurostat connector failed: {e}")
        return False

def test_imports():
    """Test that all required packages can be imported"""
    imports_to_test = [
        'pandas',
        'clickhouse_connect', 
        'pydantic',
        'aiohttp',
        'scrapy',
        'typer'
    ]
    
    success_count = 0
    for module in imports_to_test:
        try:
            __import__(module)
            print(f"✅ {module} imported successfully")
            success_count += 1
        except ImportError as e:
            print(f"❌ Failed to import {module}: {e}")
    
    print(f"\n📊 Import test: {success_count}/{len(imports_to_test)} successful")
    return success_count == len(imports_to_test)

def test_services():
    """Test external services connectivity"""
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    # Create session with retry strategy
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    
    services = {
        "ClickHouse": ("http://localhost:8124/ping", 200),
        "Airflow": ("http://localhost:8081/health", 200),
        "Jupyter": ("http://localhost:8888/", 200)
    }
    
    results = {}
    for service, (url, expected_status) in services.items():
        try:
            response = session.get(url, timeout=5)
            if response.status_code == expected_status:
                print(f"✅ {service} service is healthy")
                results[service] = True
            else:
                print(f"⚠️ {service} returned status {response.status_code}")
                results[service] = False
        except Exception as e:
            print(f"❌ {service} connection failed: {str(e)[:50]}...")
            results[service] = False
    
    return all(results.values())

async def main():
    """Run all tests"""
    print("🔍 Testing BnB Data4Transformation Components\n")
    
    # Test imports
    print("1. Testing package imports:")
    imports_ok = test_imports()
    
    print("\n2. Testing external services:")
    services_ok = test_services()
    
    print("\n3. Testing ClickHouse connection:")
    clickhouse_ok = test_clickhouse_connection()
    
    print("\n4. Testing DAWUM connector:")
    dawum_ok = await test_dawum_connector()
    
    print("\n5. Testing Destatis connector:")
    destatis_ok = await test_destatis_connector()

    print("\n6. Testing Eurostat connector:")
    eurostat_ok = await test_eurostat_connector()
    
    # Summary
    print("\n" + "="*50)
    print("📋 Test Summary:")
    print(f"   Imports: {'✅' if imports_ok else '❌'}")
    print(f"   Services: {'✅' if services_ok else '❌'}")
    print(f"   ClickHouse: {'✅' if clickhouse_ok else '❌'}")
    print(f"   DAWUM: {'✅' if dawum_ok else '❌'}")
    print(f"   Destatis: {'✅' if destatis_ok else '❌'}")
    print(f"   Eurostat: {'✅' if eurostat_ok else '❌'}")

    all_ok = all([imports_ok, services_ok, clickhouse_ok, dawum_ok, destatis_ok, eurostat_ok])
    print(f"\n🎯 Overall status: {'✅ All systems operational!' if all_ok else '❌ Some issues found'}")
    
    return all_ok

if __name__ == "__main__":
    asyncio.run(main())
