"""
Integration tests for DAWUM data pipeline.
Tests end-to-end data flow from extraction to ClickHouse loading.
"""

import pytest
import asyncio
import os
from unittest.mock import patch
from datetime import datetime

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from connectors.dawum_connector import DawumConnector, DawumConfig
from elt.loader_clickhouse import ClickHouseLoader, ClickHouseConfig


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("INTEGRATION_TESTS") != "true",
    reason="Integration tests disabled. Set INTEGRATION_TESTS=true to enable."
)
class TestDawumIntegration:
    """Integration tests for DAWUM pipeline."""
    
    @pytest.fixture
    def clickhouse_config(self):
        """ClickHouse configuration for tests."""
        return ClickHouseConfig(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database="test_analytics"  # Use test database
        )
    
    @pytest.fixture
    def test_poll_data(self):
        """Test poll data for insertion."""
        return [
            {
                'poll_id': 'test_poll_001',
                'institute_id': 'test_inst_1',
                'institute_name': 'Test Institute 1',
                'tasker_id': 'test_task_1',
                'tasker_name': 'Test Tasker 1',
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
                'source_loaded_at': datetime.utcnow().isoformat(),
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            },
            {
                'poll_id': 'test_poll_002',
                'institute_id': 'test_inst_2',
                'institute_name': 'Test Institute 2',
                'tasker_id': 'test_task_2',
                'tasker_name': 'Test Tasker 2',
                'parliament_id': 'bundestag',
                'parliament_name': 'Bundestag',
                'method_id': 'phone',
                'method_name': 'Phone Survey',
                'survey_period_start': '2024-01-02 00:00:00',
                'survey_period_end': '2024-01-04 23:59:59',
                'publication_date': '2024-01-06 10:00:00',
                'sample_size': 1500,
                'results': '[{"party_id": "spd", "party_name": "SPD", "percentage": 23.0}]',
                'source_url': 'https://example.com/test2',
                'source_loaded_at': datetime.utcnow().isoformat(),
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }
        ]
    
    def test_clickhouse_connection(self, clickhouse_config):
        """Test ClickHouse connection and basic operations."""
        
        with ClickHouseLoader(clickhouse_config) as loader:
            # Test connection
            loader.client.ping()
            
            # Create test database
            loader.client.command("CREATE DATABASE IF NOT EXISTS test_analytics")
    
    def test_table_creation_and_data_loading(self, clickhouse_config, test_poll_data):
        """Test table creation and data loading."""
        
        with ClickHouseLoader(clickhouse_config) as loader:
            # Create test database
            loader.client.command("CREATE DATABASE IF NOT EXISTS test_analytics")
            loader.client.command("USE test_analytics")
            
            # Create raw schema
            loader.client.command("CREATE DATABASE IF NOT EXISTS raw")
            
            # Create table using the upsert method (which includes table creation)
            rows_loaded = loader.upsert_dawum_polls(test_poll_data)
            
            assert rows_loaded == len(test_poll_data)
            
            # Verify data was loaded
            result = loader.client.query("SELECT COUNT(*) FROM raw.dawum_polls")
            count = result.result_rows[0][0]
            assert count >= len(test_poll_data)
            
            # Verify specific data
            result = loader.client.query(
                "SELECT poll_id, institute_name, sample_size FROM raw.dawum_polls WHERE poll_id = 'test_poll_001'"
            )
            rows = result.result_rows
            assert len(rows) == 1
            assert rows[0][0] == 'test_poll_001'
            assert rows[0][1] == 'Test Institute 1'
            assert rows[0][2] == 1000
    
    def test_upsert_functionality(self, clickhouse_config, test_poll_data):
        """Test upsert functionality with duplicate data."""
        
        with ClickHouseLoader(clickhouse_config) as loader:
            # Create test database
            loader.client.command("CREATE DATABASE IF NOT EXISTS test_analytics")
            loader.client.command("USE test_analytics")
            
            # First load
            rows_loaded_1 = loader.upsert_dawum_polls(test_poll_data)
            assert rows_loaded_1 == len(test_poll_data)
            
            # Modify one record and load again
            updated_data = test_poll_data.copy()
            updated_data[0]['sample_size'] = 2000
            updated_data[0]['updated_at'] = datetime.utcnow().isoformat()
            
            rows_loaded_2 = loader.upsert_dawum_polls(updated_data)
            assert rows_loaded_2 == len(updated_data)
            
            # Verify the update took effect (after optimization)
            loader.client.command("OPTIMIZE TABLE raw.dawum_polls FINAL")
            
            result = loader.client.query(
                "SELECT sample_size FROM raw.dawum_polls WHERE poll_id = 'test_poll_001' ORDER BY updated_at DESC LIMIT 1"
            )
            
            # Note: ReplacingMergeTree may need time to merge, 
            # so this test might need adjustment based on ClickHouse behavior
            sample_size = result.result_rows[0][0]
            assert sample_size == 2000
    
    def test_batch_loading_large_dataset(self, clickhouse_config):
        """Test batch loading with larger dataset."""
        
        # Generate larger test dataset
        large_dataset = []
        for i in range(1000):
            poll = {
                'poll_id': f'batch_test_poll_{i:04d}',
                'institute_id': f'inst_{i % 10}',
                'institute_name': f'Institute {i % 10}',
                'tasker_id': f'task_{i % 5}',
                'tasker_name': f'Tasker {i % 5}',
                'parliament_id': 'bundestag',
                'parliament_name': 'Bundestag',
                'method_id': 'online',
                'method_name': 'Online',
                'survey_period_start': '2024-01-01 00:00:00',
                'survey_period_end': '2024-01-03 23:59:59',
                'publication_date': f'2024-01-{5 + (i % 20):02d} 10:00:00',
                'sample_size': 1000 + i,
                'results': f'[{{"party_id": "party_{i % 5}", "percentage": {20 + (i % 30)}}}]',
                'source_url': f'https://example.com/poll{i}',
                'source_loaded_at': datetime.utcnow().isoformat(),
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }
            large_dataset.append(poll)
        
        with ClickHouseLoader(clickhouse_config) as loader:
            # Create test database
            loader.client.command("CREATE DATABASE IF NOT EXISTS test_analytics")
            loader.client.command("USE test_analytics")
            
            # Load large dataset
            rows_loaded = loader.upsert_dawum_polls(large_dataset)
            assert rows_loaded == len(large_dataset)
            
            # Verify count
            result = loader.client.query("SELECT COUNT(*) FROM raw.dawum_polls WHERE poll_id LIKE 'batch_test_poll_%'")
            count = result.result_rows[0][0]
            assert count >= len(large_dataset)
    
    def test_cleanup(self, clickhouse_config):
        """Clean up test data after tests."""
        
        with ClickHouseLoader(clickhouse_config) as loader:
            try:
                # Clean up test data
                loader.client.command("DROP TABLE IF EXISTS raw.dawum_polls")
                loader.client.command("DROP DATABASE IF EXISTS raw")
                loader.client.command("DROP DATABASE IF EXISTS test_analytics")
            except Exception as e:
                # Cleanup failures shouldn't fail the test
                print(f"Cleanup warning: {e}")


if __name__ == "__main__":
    # Run integration tests manually
    import subprocess
    import sys
    
    # Set environment variable for integration tests
    os.environ["INTEGRATION_TESTS"] = "true"
    
    # Run the tests
    subprocess.run([
        sys.executable, "-m", "pytest", 
        "-v", "-m", "integration",
        __file__
    ])
