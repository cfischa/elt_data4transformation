"""
Unit tests for GENESIS-Online (Destatis) connector.
Tests data extraction, chunking, authentication, and error handling with mocked responses.
"""

import pytest
import gzip
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime
from pathlib import Path

import httpx
import orjson

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from connectors.destatis_connector import (
    DestatisConnector, 
    DestatisConfig, 
    TableInfo, 
    BASE_ENDPOINT, 
    ENDPOINT_DATA_TABLE, 
    ENDPOINT_METADATA_TABLE,
    DestatisInvalidContent
)


class TestDestatisConnector:
    """Test GENESIS-Online connector functionality."""
    
    @pytest.fixture
    def config(self):
        """Test configuration with API token."""
        return DestatisConfig(
            api_token="test_token_123",
            rate_limit_requests=100,
            rate_limit_period=60,
            timeout=30,
            max_retries=2
        )
    
    @pytest.fixture
    def config_username_password(self):
        """Test configuration with username/password."""
        return DestatisConfig(
            username="test_user",
            password="test_pass",
            rate_limit_requests=100,
            rate_limit_period=60,
            timeout=30,
            max_retries=2
        )
    
    @pytest.fixture
    def config_anonymous(self):
        """Test configuration without authentication."""
        return DestatisConfig(
            rate_limit_requests=50,
            rate_limit_period=60,
            timeout=30
        )
    
    @pytest.fixture
    def connector(self, config):
        """Test connector instance."""
        return DestatisConnector(config)
    
    @pytest.fixture
    def mock_table_metadata(self):
        """Mock table metadata response."""
        return {
            "Object": {
                "Code": "12411-0001",
                "Content": "Population by age groups",
                "Updated": "15.01.2024 14:30:00",
                "Variable": ["AGE", "SEX", "NATIONALITY"],
                "Size": 50000
            }
        }
    
    @pytest.fixture
    def mock_table_data_json(self):
        """Mock JSON-stat table data response."""
        return {
            "dataset": {
                "dimension": {
                    "id": ["time", "age", "sex"],
                    "size": [5, 10, 2],
                    "time": {
                        "category": {
                            "index": {"2019": 0, "2020": 1, "2021": 2, "2022": 3, "2023": 4}
                        }
                    },
                    "age": {
                        "category": {
                            "index": {
                                "0-4": 0, "5-9": 1, "10-14": 2, "15-19": 3, "20-24": 4,
                                "25-29": 5, "30-34": 6, "35-39": 7, "40-44": 8, "45+"  : 9
                            }
                        }
                    },
                    "sex": {
                        "category": {
                            "index": {"male": 0, "female": 1}
                        }
                    }
                },
                "value": [
                    1000, 1100, 1200, 1300, 1400,  # 2019-2023 for age group 0-4, male
                    950, 1050, 1150, 1250, 1350,   # 2019-2023 for age group 0-4, female
                    # ... more data points
                ]
            }
        }
    
    @pytest.fixture
    def mock_table_data_csv(self):
        """Mock CSV table data response."""
        return """Year,Age_Group,Sex,Population
2019,0-4,male,1000
2019,0-4,female,950
2019,5-9,male,1100
2019,5-9,female,1050
2020,0-4,male,1100
2020,0-4,female,1000
2020,5-9,male,1150
2020,5-9,female,1100"""
    
    @pytest.fixture
    def mock_table_list(self):
        """Mock table list response."""
        return {
            "List": [
                {
                    "Code": "12411-0001",
                    "Content": "Population by age groups",
                    "Updated": "15.01.2024 14:30:00"
                },
                {
                    "Code": "12411-0002", 
                    "Content": "Population by citizenship",
                    "Updated": "10.01.2024 10:15:00"
                }
            ]
        }
    
    # Test URL building
    def test_build_url(self, connector):
        """Test URL building helper method."""
        assert connector._build_url("data/table") == f"{BASE_ENDPOINT}/data/table"
        assert connector._build_url("/data/table") == f"{BASE_ENDPOINT}/data/table"
        assert connector._build_url("helloworld/logincheck") == f"{BASE_ENDPOINT}/helloworld/logincheck"
    
    # Test authentication parameter generation
    def test_get_auth_params_with_token(self, config):
        """Test auth params generation with API token."""
        connector = DestatisConnector(config)
        auth_params = connector._get_auth_params()
        
        assert auth_params == {"username": "test_token_123", "password": ""}
    
    def test_get_auth_params_with_username_password(self, config_username_password):
        """Test auth params generation with username/password."""
        connector = DestatisConnector(config_username_password)
        auth_params = connector._get_auth_params()
        
        assert auth_params == {"username": "test_user", "password": "test_pass"}
    
    def test_get_auth_params_anonymous(self, config_anonymous):
        """Test auth params generation without credentials."""
        connector = DestatisConnector(config_anonymous)
        auth_params = connector._get_auth_params()
        
        assert auth_params == {}
    
    def test_get_auth_headers_with_username_password(self, config_username_password):
        """Test Basic Auth header generation."""
        connector = DestatisConnector(config_username_password)
        headers = connector._get_auth_headers()
        
        assert "Authorization" in headers
        assert headers["Authorization"].startswith("Basic ")
    
    def test_get_auth_headers_with_token(self, config):
        """Test that no auth headers are generated when using token."""
        connector = DestatisConnector(config)
        headers = connector._get_auth_headers()
        
        assert headers == {}
    
    # Test response validation
    def test_validate_response_content_valid_json(self, connector):
        """Test response content validation with valid JSON."""
        mock_response = MagicMock()
        mock_response.headers = {"content-type": "application/json"}
        mock_response.text = '{"test": "data"}'
        
        # Should not raise
        connector._validate_response_content(mock_response)
    
    def test_validate_response_content_invalid_html(self, connector):
        """Test response content validation with HTML content."""
        mock_response = MagicMock()
        mock_response.headers = {"content-type": "text/html"}
        mock_response.text = '<html><body>Error</body></html>'
        
        with pytest.raises(DestatisInvalidContent):
            connector._validate_response_content(mock_response)
    
    # Test JSON shape validation
    def test_validate_json_shape_valid(self, connector):
        """Test JSON shape validation with valid structure."""
        data = {
            "Identifier": "test",
            "Status": "success", 
            "Object": {"data": "test"}
        }
        
        # Should not raise
        connector._validate_json_shape(data, ENDPOINT_DATA_TABLE)
    
    def test_config_creation(self, config):
        """Test configuration creation and validation."""
        assert config.base_url == BASE_ENDPOINT
        assert config.api_token == "test_token_123"
        assert config.rate_limit_requests == 100
        assert config.timeout == 30
    
    def test_auth_headers_with_credentials(self, config_username_password):
        """Test authentication header generation with credentials."""
        connector = DestatisConnector(config_username_password)
        headers = connector._get_auth_headers()
        
        assert "Authorization" in headers
        assert headers["Authorization"].startswith("Basic ")
        
        # Decode and verify base64 encoded credentials
        import base64
        encoded = headers["Authorization"].split(" ")[1]
        decoded = base64.b64decode(encoded).decode()
        assert decoded == "test_user:test_pass"
    
    def test_auth_headers_without_credentials(self, config_anonymous):
        """Test authentication header generation without credentials."""
        connector = DestatisConnector(config_anonymous)
        headers = connector._get_auth_headers()
        
        assert headers == {}
    
    def test_auth_headers_with_api_token(self):
        """Test authentication header generation with API token."""
        config = DestatisConfig(api_token="d276cd2f1bc74bb780fd484cb5fd0fa6")
        connector = DestatisConnector(config)
        headers = connector._get_auth_headers()
        
        # With token, no Authorization header should be generated (token goes in query params)
        assert headers == {}
    
    @pytest.mark.asyncio
    async def test_table_info_parsing(self, connector, mock_table_metadata):
        """Test table metadata parsing."""
        # Mock the HTTP response
        with patch.object(connector, '_make_request') as mock_request:
            mock_response = MagicMock()
            mock_response.json.return_value = mock_table_metadata
            mock_request.return_value = mock_response
            
            async with connector:
                table_info = await connector.get_table_info("12411-0001")
                
                assert table_info.name == "12411-0001"
                assert table_info.description == "Population by age groups"
                assert table_info.variables == ["AGE", "SEX", "NATIONALITY"]
                assert table_info.size_estimate == 50000
                assert table_info.updated is not None
    
    def test_datetime_parsing(self, connector):
        """Test datetime parsing from GENESIS format."""
        # Valid datetime
        dt = connector._parse_datetime("15.01.2024 14:30:00")
        assert dt == datetime(2024, 1, 15, 14, 30, 0)
        
        # Invalid datetime
        dt_invalid = connector._parse_datetime("invalid_date")
        assert dt_invalid is None
        
        # None input
        dt_none = connector._parse_datetime(None)
        assert dt_none is None
    
    def test_chunk_calculation(self, connector):
        """Test chunking logic for large tables."""
        # No years specified
        chunks = connector._calculate_chunks(None, None)
        assert chunks == [{}]
        
        # Small time range
        chunks = connector._calculate_chunks(2020, 2023)
        assert len(chunks) == 1
        assert chunks[0] == {"startYear": "2020", "endYear": "2023"}
        
        # Large time range  
        chunks = connector._calculate_chunks(2000, 2023)
        assert len(chunks) > 1
        
        # Very large estimated size
        chunks = connector._calculate_chunks(2020, 2023, estimated_size=2000000)
        assert len(chunks) >= 1
    
    @pytest.mark.asyncio
    async def test_successful_table_extraction_json(self, connector, mock_table_metadata, mock_table_data_json):
        """Test successful table extraction with JSON format using POST method."""
        
        with patch.object(connector, '_make_request') as mock_request, \
             patch.object(connector, '_save_combined_data') as mock_save:
            
            # Mock metadata response
            metadata_response = MagicMock()
            metadata_response.json.return_value = mock_table_metadata
            
            # Mock data response
            data_response = MagicMock()
            data_response.content = orjson.dumps(mock_table_data_json)
            data_response.headers = {"content-encoding": "gzip"}
            
            # Compress the content to simulate gzip
            compressed_content = gzip.compress(data_response.content)
            data_response.content = compressed_content
            
            # Configure mock to return different responses based on endpoint and data
            def mock_request_side_effect(endpoint, data=None):
                if endpoint == "metadata/table":
                    return metadata_response
                elif endpoint == "data/table":
                    return data_response
                else:
                    raise ValueError(f"Unexpected endpoint: {endpoint}")
            
            mock_request.side_effect = mock_request_side_effect
            mock_save.return_value = Path("/tmp/test_table.json")
            
            async with connector:
                result_path = await connector.fetch_table(
                    table_id="12411-0001",
                    area="de",
                    start_year=2020,
                    end_year=2023,
                    fmt="json"
                )
                
                assert result_path == Path("/tmp/test_table.json")
                
                # Verify calls use POST method with data payloads
                assert mock_request.call_count == 2  # metadata + data
                
                # Check that metadata request was made with POST data
                metadata_call = mock_request.call_args_list[0]
                assert metadata_call[0][0] == "metadata/table"
                assert metadata_call[0][1]["name"] == "12411-0001"
                
                # Check that data request was made with POST data  
                data_call = mock_request.call_args_list[1]
                assert data_call[0][0] == "data/table"
                data_payload = data_call[0][1]
                assert data_payload["name"] == "12411-0001"
                assert data_payload["area"] == "de"
                assert data_payload["format"] == "json"
                
                mock_save.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_table_extraction_with_chunking(self, connector, mock_table_metadata, mock_table_data_json):
        """Test table extraction that requires chunking using POST method."""
        
        with patch.object(connector, '_make_request') as mock_request, \
             patch.object(connector, '_save_combined_data') as mock_save, \
             patch.object(connector, '_calculate_chunks') as mock_chunks:
            
            # Mock chunks to simulate multiple requests
            mock_chunks.return_value = [
                {"startYear": "2020", "endYear": "2021"},
                {"startYear": "2022", "endYear": "2023"}
            ]
            
            # Mock metadata response
            metadata_response = MagicMock()
            metadata_response.json.return_value = mock_table_metadata
            
            # Mock data response
            data_response = MagicMock()
            data_response.content = orjson.dumps(mock_table_data_json)
            data_response.headers = {}
            
            def mock_request_side_effect(endpoint, data=None):
                if endpoint == "metadata/table":
                    return metadata_response
                elif endpoint == "data/table":
                    return data_response
                else:
                    raise ValueError(f"Unexpected endpoint: {endpoint}")
            
            mock_request.side_effect = mock_request_side_effect
            mock_save.return_value = Path("/tmp/chunked_table.json")
            
            async with connector:
                result_path = await connector.fetch_table(
                    table_id="12411-0001",
                    area="de", 
                    start_year=2020,
                    end_year=2023,
                    fmt="json"
                )
                
                assert result_path == Path("/tmp/chunked_table.json")
                
                # Verify multiple data requests (one for each chunk)
                data_calls = [call for call in mock_request.call_args_list 
                             if call[0][0] == "data/table"]
                assert len(data_calls) == 2  # Two chunks
                
                # Verify each chunk request has correct data
                for i, call in enumerate(data_calls):
                    data_payload = call[0][1]
                    assert data_payload["name"] == "12411-0001"
                    assert "startYear" in data_payload
                    assert "endYear" in data_payload
    
    @pytest.mark.asyncio
    async def test_table_extraction_csv_format(self, connector, mock_table_metadata, mock_table_data_csv):
        """Test table extraction with CSV format."""
        
        with patch.object(connector, '_make_request') as mock_request, \
             patch.object(connector, '_save_combined_data') as mock_save:
            
            # Mock metadata response
            metadata_response = MagicMock()
            metadata_response.json.return_value = mock_table_metadata
            
            # Mock CSV data response
            data_response = MagicMock()
            data_response.content = mock_table_data_csv.encode('utf-8')
            data_response.headers = {}
            
            def mock_request_side_effect(endpoint, params=None):
                if endpoint.startswith("metadata/table"):
                    return metadata_response
                elif endpoint == "data/table":
                    return data_response
                else:
                    raise ValueError(f"Unexpected endpoint: {endpoint}")
            
            mock_request.side_effect = mock_request_side_effect
            mock_save.return_value = Path("/tmp/test_table.csv")
            
            async with connector:
                result_path = await connector.fetch_table(
                    table_id="12411-0001",
                    fmt="csv"
                )
                
                assert result_path == Path("/tmp/test_table.csv")
                mock_save.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_http_503_retry(self, connector):
        """Test retry logic on HTTP 503 errors."""
        
        with patch.object(connector, '_client') as mock_client:
            # Mock responses: first fails with 503, second succeeds
            error_response = httpx.Response(
                status_code=503,
                request=httpx.Request("GET", "http://test.com")
            )
            success_response = httpx.Response(
                status_code=200,
                headers={"content-type": "application/json"},
                content=b'{"result": "ok"}',
                request=httpx.Request("GET", "http://test.com")
            )
            
            mock_client.post = AsyncMock(side_effect=[
                error_response,
                success_response
            ])
            
            async with connector:
                await connector._make_request("test/endpoint")
                
                assert mock_client.post.call_count == 2
    
    @pytest.mark.asyncio
    async def test_rate_limiting_429(self, connector):
        """Test rate limiting handling on HTTP 429."""
        
        with patch.object(connector, '_client') as mock_client, \
             patch('asyncio.sleep') as mock_sleep:
            
            # Mock 429 response with Retry-After header
            rate_limit_response = httpx.Response(
                status_code=429,
                headers={"Retry-After": "60"},
                request=httpx.Request("GET", "http://test.com")
            )
            success_response = httpx.Response(
                status_code=200,
                headers={"content-type": "application/json"},
                content=b'{"result": "ok"}',
                request=httpx.Request("GET", "http://test.com")
            )
            
            mock_client.post = AsyncMock(side_effect=[
                rate_limit_response,
                success_response
            ])
            
            async with connector:
                with pytest.raises(httpx.HTTPStatusError):
                    await connector._make_request("test/endpoint")
                
                mock_sleep.assert_called_with(60)
    
    @pytest.mark.asyncio
    async def test_get_available_tables(self, connector, mock_table_list):
        """Test getting list of available tables using POST method."""
        
        with patch.object(connector, '_make_request') as mock_request:
            mock_response = MagicMock()
            mock_response.json.return_value = mock_table_list
            mock_request.return_value = mock_response
            
            async with connector:
                tables = await connector.get_available_tables()
                
                assert len(tables) == 2
                assert tables[0].name == "12411-0001"
                assert tables[0].description == "Population by age groups"
                assert tables[1].name == "12411-0002"
                
                # Verify POST request with empty data
                mock_request.assert_called_once_with("find/table", {})
    
    @pytest.mark.asyncio
    async def test_get_available_tables_with_filter(self, connector, mock_table_list):
        """Test getting filtered list of available tables using POST method."""
        
        with patch.object(connector, '_make_request') as mock_request:
            mock_response = MagicMock()
            mock_response.json.return_value = mock_table_list
            mock_request.return_value = mock_response
            
            async with connector:
                tables = await connector.get_available_tables(filter_term="population")
                
                assert len(tables) == 2
                # Verify POST request with filter data
                mock_request.assert_called_once_with("find/table", {"term": "population"})
    
    def test_merge_jsonstat_datasets(self, connector, mock_table_data_json):
        """Test merging multiple JSON-stat datasets."""
        
        # Single dataset
        result = connector._merge_jsonstat_datasets([mock_table_data_json])
        assert result == mock_table_data_json
        
        # Multiple datasets
        dataset2 = {
            "dataset": {
                "value": {"additional": "data"}
            }
        }
        
        result = connector._merge_jsonstat_datasets([mock_table_data_json, dataset2])
        assert "dataset" in result
        assert "value" in result["dataset"]
    
    def test_merge_csv_chunks(self, connector):
        """Test merging CSV chunks."""
        
        # Single chunk
        chunk1 = "Year,Population\n2020,1000\n2021,1100"
        result = connector._merge_csv_chunks([chunk1])
        assert result == chunk1
        
        # Multiple chunks
        chunk2 = "Year,Population\n2022,1200\n2023,1300"
        
        result = connector._merge_csv_chunks([chunk1, chunk2])
        expected = "Year,Population\n2020,1000\n2021,1100\n2022,1200\n2023,1300"
        assert result == expected
    
    @pytest.mark.asyncio
    async def test_fetch_data_generator(self, connector):
        """Test the fetch_data generator method."""
        
        with patch.object(connector, 'fetch_table') as mock_fetch:
            mock_fetch.return_value = Path("/tmp/test_table.json")
            
            async with connector:
                results = []
                async for data in connector.fetch_data(
                    table_ids=["12411-0001", "12411-0002"],
                    area="de"
                ):
                    results.append(data)
                
                assert len(results) == 2
                assert all(result["status"] == "success" for result in results)
                assert mock_fetch.call_count == 2
    
    @pytest.mark.asyncio
    async def test_fetch_data_error_handling(self, connector):
        """Test error handling in fetch_data generator."""
        
        with patch.object(connector, 'fetch_table') as mock_fetch:
            mock_fetch.side_effect = Exception("Test error")
            
            async with connector:
                results = []
                async for data in connector.fetch_data(table_ids=["12411-0001"]):
                    results.append(data)
                
                assert len(results) == 1
                assert results[0]["status"] == "failed"
                assert "Test error" in results[0]["error"]
    
    @pytest.mark.asyncio
    async def test_get_incremental_data(self, connector, mock_table_list):
        """Test incremental data fetching."""
        
        with patch.object(connector, 'get_available_tables') as mock_tables, \
             patch.object(connector, 'fetch_data') as mock_fetch:
            
            # Mock tables with different update times
            tables = [
                TableInfo(
                    name="12411-0001",
                    updated=datetime(2024, 1, 15)
                ),
                TableInfo(
                    name="12411-0002", 
                    updated=datetime(2023, 12, 1)  # Older
                )
            ]
            mock_tables.return_value = tables
            
            # Mock fetch_data to return async generator
            async def mock_fetch_gen(**kwargs):
                # Get table_ids from kwargs, handling both list and direct access
                table_ids = kwargs.get("table_ids", [])
                if table_ids:
                    yield {"table_id": table_ids[0], "status": "success"}
            
            # Set the mock to actually call the generator
            mock_fetch.side_effect = lambda **kwargs: mock_fetch_gen(**kwargs)
            
            async with connector:
                since = datetime(2024, 1, 1)
                results = []
                
                async for data in connector.get_incremental_data(since):
                    results.append(data)
                
                # Should only get the newer table
                assert len(results) == 1
                assert results[0]["table_id"] == "12411-0001"
