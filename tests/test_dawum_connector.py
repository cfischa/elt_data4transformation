"""
Unit tests for DAWUM connector.
Tests data extraction, transformation, and error handling with mocked responses.
"""

import pytest
import asyncio
import json
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from connectors.dawum_connector import DawumConnector, DawumConfig


class TestDawumConnector:
    """Test DAWUM connector functionality."""
    
    @pytest.fixture
    def config(self):
        """Test configuration."""
        return DawumConfig(
            base_url="https://api.test.dawum.de",
            api_key="test_key",
            rate_limit_requests=100,
            rate_limit_period=60,
            timeout=10
        )
    
    @pytest.fixture
    def connector(self, config):
        """Test connector instance."""
        return DawumConnector(config)
    
    @pytest.fixture
    def mock_poll_data(self):
        """Mock poll data response."""
        return {
            'data': [
                {
                    'id': 'poll_123',
                    'institute': {'id': 'inst_1', 'name': 'Test Institute'},
                    'tasker': {'id': 'task_1', 'name': 'Test Tasker'},
                    'parliament': {'id': 'parl_1', 'name': 'Bundestag'},
                    'method': {'id': 'method_1', 'name': 'Online'},
                    'survey_period': {
                        'start': '2024-01-01',
                        'end': '2024-01-03'
                    },
                    'publication_date': '2024-01-05',
                    'sample_size': 1000,
                    'results': [
                        {
                            'party': {'id': 'cdu', 'name': 'CDU/CSU', 'short': 'CDU'},
                            'percentage': 25.5,
                            'seats': None,
                            'change': 1.2
                        },
                        {
                            'party': {'id': 'spd', 'name': 'SPD', 'short': 'SPD'},
                            'percentage': 23.0,
                            'seats': None,
                            'change': -0.8
                        }
                    ],
                    'source_url': 'https://example.com/poll123'
                },
                {
                    'id': 'poll_124',
                    'institute': {'id': 'inst_2', 'name': 'Another Institute'},
                    'tasker': {'id': 'task_2', 'name': 'Another Tasker'},
                    'parliament': {'id': 'parl_1', 'name': 'Bundestag'},
                    'method': {'id': 'method_2', 'name': 'Phone'},
                    'survey_period': {
                        'start': '2024-01-02',
                        'end': '2024-01-04'
                    },
                    'publication_date': '2024-01-06',
                    'sample_size': 1500,
                    'results': [
                        {
                            'party': {'id': 'cdu', 'name': 'CDU/CSU', 'short': 'CDU'},
                            'percentage': 26.0,
                            'seats': None,
                            'change': 0.5
                        }
                    ],
                    'source_url': 'https://example.com/poll124'
                }
            ]
        }
    
    @pytest.mark.asyncio
    async def test_fetch_polls_success(self, connector, mock_poll_data):
        """Test successful poll data extraction."""
        
        # Mock the HTTP session and response
        with patch('aiohttp.ClientSession') as mock_session:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=mock_poll_data)
            mock_response.raise_for_status = MagicMock()
            
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value = mock_response
            
            # Test data extraction
            polls = await connector.fetch_polls(limit=10)
            
            # Assertions
            assert len(polls) == 2
            assert polls[0]['poll_id'] == 'poll_123'
            assert polls[0]['institute_name'] == 'Test Institute'
            assert polls[0]['sample_size'] == 1000
            assert 'source_loaded_at' in polls[0]
            assert 'created_at' in polls[0]
            assert 'updated_at' in polls[0]
    
    @pytest.mark.asyncio
    async def test_fetch_polls_pagination(self, connector):
        """Test pagination handling."""
        
        # Mock pagination responses
        page1_data = {'data': [{'id': f'poll_{i}'} for i in range(100)]}
        page2_data = {'data': [{'id': f'poll_{i}'} for i in range(100, 150)]}
        page3_data = {'data': []}  # Empty page indicates end
        
        responses = [page1_data, page2_data, page3_data]
        response_iter = iter(responses)
        
        with patch('aiohttp.ClientSession') as mock_session:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(side_effect=lambda: next(response_iter))
            mock_response.raise_for_status = MagicMock()
            
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value = mock_response
            
            # Test pagination
            polls = await connector.fetch_polls()
            
            # Should have collected all polls from pages 1 and 2
            assert len(polls) == 150
    
    @pytest.mark.asyncio
    async def test_fetch_polls_rate_limiting(self, connector):
        """Test rate limiting behavior."""
        
        with patch('aiohttp.ClientSession') as mock_session:
            # First request returns rate limit error
            mock_response_rate_limited = AsyncMock()
            mock_response_rate_limited.status = 429
            mock_response_rate_limited.headers = {'Retry-After': '1'}
            
            # Second request succeeds
            mock_response_success = AsyncMock()
            mock_response_success.status = 200
            mock_response_success.json = AsyncMock(return_value={'data': []})
            mock_response_success.raise_for_status = MagicMock()
            
            responses = [mock_response_rate_limited, mock_response_success]
            response_iter = iter(responses)
            
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value = next(response_iter)
            
            # Mock sleep to avoid actual delays in tests
            with patch('asyncio.sleep') as mock_sleep:
                # This should eventually succeed after retries
                polls = await connector.fetch_polls()
                
                # Verify sleep was called for rate limiting
                mock_sleep.assert_called()
    
    def test_transform_poll(self, connector):
        """Test poll data transformation."""
        
        raw_poll = {
            'id': 'test_poll',
            'institute': {'id': 'inst_1', 'name': 'Test Institute'},
            'tasker': {'id': 'task_1', 'name': 'Test Tasker'},
            'parliament': {'id': 'parl_1', 'name': 'Bundestag'},
            'method': {'id': 'method_1', 'name': 'Online'},
            'survey_period': {
                'start': '2024-01-01',
                'end': '2024-01-03'
            },
            'publication_date': '2024-01-05',
            'sample_size': 1000,
            'results': [
                {
                    'party': {'id': 'cdu', 'name': 'CDU/CSU', 'short': 'CDU'},
                    'percentage': 25.5,
                    'seats': None,
                    'change': 1.2
                }
            ],
            'source_url': 'https://example.com/test'
        }
        
        source_loaded_at = "2024-01-05T10:00:00"
        transformed = connector._transform_poll(raw_poll, source_loaded_at)
        
        # Verify transformation
        assert transformed['poll_id'] == 'test_poll'
        assert transformed['institute_id'] == 'inst_1'
        assert transformed['institute_name'] == 'Test Institute'
        assert transformed['sample_size'] == 1000
        assert transformed['source_loaded_at'] == source_loaded_at
        
        # Verify results transformation
        results = json.loads(transformed['results'])
        assert len(results) == 1
        assert results[0]['party_id'] == 'cdu'
        assert results[0]['percentage'] == 25.5
    
    def test_parse_date(self, connector):
        """Test date parsing functionality."""
        
        # Test valid date formats
        assert connector._parse_date('2024-01-01') is not None
        assert connector._parse_date('2024-01-01T10:00:00') is not None
        
        # Test invalid dates
        assert connector._parse_date(None) is None
        assert connector._parse_date('') is None
        
        # Test malformed dates (should return original string with warning)
        result = connector._parse_date('invalid-date')
        assert result == 'invalid-date'
    
    def test_transform_results(self, connector):
        """Test poll results transformation."""
        
        results = [
            {
                'party': {'id': 'cdu', 'name': 'CDU/CSU', 'short': 'CDU'},
                'percentage': 25.5,
                'seats': 200,
                'change': 1.2
            },
            {
                'party': {'id': 'spd', 'name': 'SPD', 'short': 'SPD'},
                'percentage': 23.0,
                'seats': 180,
                'change': -0.8
            }
        ]
        
        transformed_json = connector._transform_results(results)
        transformed_list = json.loads(transformed_json)
        
        assert len(transformed_list) == 2
        assert transformed_list[0]['party_id'] == 'cdu'
        assert transformed_list[0]['percentage'] == 25.5
        assert transformed_list[1]['party_id'] == 'spd'
        assert transformed_list[1]['change'] == -0.8
    
    def test_sync_wrapper(self, connector):
        """Test synchronous wrapper function."""
        
        with patch.object(connector, 'fetch_polls') as mock_fetch:
            mock_fetch.return_value = [{'poll_id': 'test'}]
            
            result = connector.run_sync(limit=5)
            
            assert result == [{'poll_id': 'test'}]
            # Note: In real test this would verify asyncio.run was called
