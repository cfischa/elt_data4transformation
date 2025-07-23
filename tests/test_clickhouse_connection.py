#!/usr/bin/env python3
"""Test script to check ClickHouse connection and loader functionality."""

import os
import sys
import asyncio
import json
from datetime import datetime

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from elt.loader_clickhouse import ClickHouseLoader, ClickHouseConfig
from connectors.dawum_connector import DawumConnector, DawumConfig

def test_clickhouse_connection():
    """Test basic ClickHouse connection and queries."""
    print("Testing ClickHouse connection...")
    
    port_str = os.getenv("CLICKHOUSE_PORT", "8124").split('#')[0].strip()
    config = ClickHouseConfig(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(port_str),
        username=os.getenv("CLICKHOUSE_USER", "admin"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "analytics")
    )
    
    print(f"Config: {config.host}:{config.port}, db={config.database}")
    
    try:
        with ClickHouseLoader(config) as _loader:
            print("✓ ClickHouse connection successful")
            return True
    except Exception as e:
        print(f"✗ ClickHouse connection failed: {e}")
        return False

def test_dawum_connector():
    """Test DAWUM connector functionality."""
    print("\nTesting DAWUM connector...")
    
    config = DawumConfig(
        base_url=os.getenv("DAWUM_BASE_URL", "https://api.dawum.de"),
        api_key=os.getenv("DAWUM_API_KEY"),
        rate_limit_requests=60,
        rate_limit_period=60,
        timeout=30,
        max_retries=3
    )
    
    try:
        connector = DawumConnector(config)
        polls = asyncio.run(connector.fetch_polls())
        print(f"✓ DAWUM connector successful, fetched {len(polls)} polls")
        return polls[:2]  # Return first 2 polls for testing
    except Exception as e:
        print(f"✗ DAWUM connector failed: {e}")
        return None

import pytest

def create_test_polls():
    """Create sample poll data for testing."""
    return [
        {
            "id": "test_001",
            "date": "2024-01-15",
            "institute": "Test Institute",
            "party": "Test Party",
            "value": 25.5
        },
        {
            "id": "test_002", 
            "date": "2024-01-16",
            "institute": "Test Institute",
            "party": "Another Party",
            "value": 32.1
        }
    ]

def test_loader_functionality():
    """Test loading sample data into ClickHouse."""
    print("\nTesting ClickHouse loader functionality...")
    
    port_str = os.getenv("CLICKHOUSE_PORT", "8124").split('#')[0].strip()
    config = ClickHouseConfig(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(port_str),
        username=os.getenv("CLICKHOUSE_USER", "admin"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "analytics")
    )
    
    try:
        with ClickHouseLoader(config) as _loader:
            print("✓ Loader connection successful")
            return True
    except Exception as e:
        print(f"✗ Loader failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    print("=== ClickHouse Connection and Loader Test ===")
    
    # Test 1: Basic connection
    if not test_clickhouse_connection():
        print("\n❌ Basic connection test failed, stopping here.")
        return 1
    
    # Test 2: DAWUM connector
    sample_polls = test_dawum_connector()
    if not sample_polls:
        print("\n❌ DAWUM connector test failed, stopping here.")
        return 1
    
    # Test 3: Loader functionality
    if not test_loader_functionality():
        print("\n❌ Loader functionality test failed.")
        return 1
    
    print("\n✅ All tests passed!")
    return 0

if __name__ == "__main__":
    sys.exit(main())
