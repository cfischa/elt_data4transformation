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
    
    config = ClickHouseConfig(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8124")),
        username=os.getenv("CLICKHOUSE_USER", "admin"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "analytics")
    )
    
    print(f"Config: {config.host}:{config.port}, db={config.database}")
    
    try:
        with ClickHouseLoader(config) as loader:
            # Test basic connection
            result = loader.client.query("SELECT 1 as test")
            print(f"✅ Connection test passed: {result.result_rows}")
            
            # Test database creation
            loader.client.command("CREATE DATABASE IF NOT EXISTS test_db")
            print("✅ Database creation test passed")
            
            # Test simple table operations
            loader.client.command("""
                CREATE TABLE IF NOT EXISTS test_db.test_table (
                    id UInt32,
                    name String
                ) ENGINE = Memory
            """)
            
            loader.client.command("INSERT INTO test_db.test_table VALUES (1, 'test')")
            
            result = loader.client.query("SELECT * FROM test_db.test_table")
            print(f"✅ Table operations test passed: {result.result_rows}")
            
            # Cleanup
            loader.client.command("DROP TABLE test_db.test_table")
            print("✅ All tests passed!")
            
    except Exception as e:
        print(f"❌ ClickHouse test failed: {e}")
        return False
    
    return True


def test_loader_functionality(sample_polls):
    """Test loading sample data into ClickHouse."""
    print("\nTesting ClickHouse loader functionality...")
    
    config = ClickHouseConfig(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8124")),
        username=os.getenv("CLICKHOUSE_USER", "admin"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "analytics")
    )
    
    try:
        with ClickHouseLoader(config) as loader:
            # Test upsert functionality
            rows_loaded = loader.upsert_dawum_polls(sample_polls)
            print(f"✅ Loaded {rows_loaded} polls successfully")
            
            # Test query functionality
            result = loader.client.query("SELECT COUNT(*) as count FROM raw.dawum_polls")
            print(f"✅ Total polls in database: {result.result_rows[0][0]}")
            
    except Exception as e:
        print(f"❌ Loader test failed: {e}")
        return False
    
    return True


def main():
    """Main test function."""
    print("=" * 50)
    print("ClickHouse Connection and Loader Tests")
    print("=" * 50)
    
    # Test basic connection
    if not test_clickhouse_connection():
        print("❌ Basic connection test failed")
        return 1
    
    # Sample poll data for testing
    sample_polls = [
        {
            'poll_id': 'test_poll_001',
            'institute_id': 'test_institute',
            'institute_name': 'Test Institute',
            'tasker_id': 'test_tasker',
            'tasker_name': 'Test Tasker',
            'parliament_id': 'bundestag',
            'parliament_name': 'Bundestag',
            'method_id': 'online',
            'method_name': 'Online Survey',
            'survey_period_start': '2024-01-01 00:00:00',
            'survey_period_end': '2024-01-03 23:59:59',
            'publication_date': '2024-01-05 10:00:00',
            'sample_size': 1000,
            'results': '[{"party_id": "cdu", "party_name": "CDU/CSU", "percentage": 25.5}]',
            'source_url': 'https://example.com/test1',
            'source_loaded_at': datetime.now().isoformat(),
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
    ]
    
    # Test loader functionality
    if not test_loader_functionality(sample_polls):
        print("❌ Loader functionality test failed")
        return 1
    
    print("\n✅ All tests passed successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
