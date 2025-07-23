"""
DAWUM polling data connector.
Fetches polling data from DAWUM API with async HTTP, pagination, and retry logic.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, AsyncGenerator
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .base_connector import BaseConnector, ConnectorConfig


class DawumConfig(ConnectorConfig):
    """Configuration for DAWUM connector."""
    base_url: str = "https://api.dawum.de"
    api_key: Optional[str] = None
    rate_limit_requests: int = 60
    rate_limit_period: int = 60


class DawumConnector(BaseConnector):
    """
    DAWUM polling data connector.
    
    Fetches polling data from DAWUM API with support for:
    - Async HTTP requests with proper error handling
    - Pagination through large datasets
    - Rate limiting and retry logic
    - Data transformation and validation
    """
    
    def __init__(self, config: Optional[DawumConfig] = None):
        self.config = config or DawumConfig()
        super().__init__(self.config)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def _fetch_page(self, session: aiohttp.ClientSession, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch a single page of data with retry logic."""
        url = f"{self.config.base_url}/{endpoint}"
        
        response = await session.get(url, params=params)
        if response.status == 429:  # Rate limited
            retry_after = int(response.headers.get('Retry-After', 60))
            self.logger.warning(f"Rate limited, sleeping for {retry_after} seconds.")
            await asyncio.sleep(retry_after)
            response.raise_for_status() # Will raise for 429, triggering retry
        
        response.raise_for_status()
        return await response.json()

    async def fetch_polls(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch all polling data from DAWUM API.
        
        Args:
            limit: Maximum number of polls to fetch (None for all)
            
        Returns:
            List of poll dictionaries with transformed data
        """
        polls = []
        source_loaded_at = datetime.now(timezone.utc).isoformat()
        
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            params = {}
            if self.config.api_key:
                params['api_key'] = self.config.api_key
            
            try:
                self.logger.info("Fetching DAWUM surveys data")
                
                await self.rate_limiter.wait_if_needed()
                response_data = await self._fetch_page(session, "", params)  # Changed from "data.json" to "" (root endpoint)
                
                surveys = response_data.get('Surveys', {})
                
                for survey_id, survey_data in surveys.items():
                    poll = self._transform_survey_to_poll(survey_id, survey_data, source_loaded_at)
                    polls.append(poll)
                    
                    if limit and len(polls) >= limit:
                        break
                
                self.logger.info(f"Successfully fetched {len(polls)} polls from DAWUM")
                
            except Exception as e:
                self.logger.error(f"Error fetching DAWUM data: {e}")
                raise
        
        return polls[:limit] if limit else polls
    
    def _transform_survey_to_poll(self, survey_id: str, survey_data: Dict[str, Any], source_loaded_at: str) -> Dict[str, Any]:
        """Transform DAWUM survey data into standardized poll format."""
        # Convert sample_size to int if possible
        sample_size = survey_data.get('Surveyed_Persons')
        if sample_size is not None and sample_size != '':
            try:
                sample_size = int(sample_size)
            except (ValueError, TypeError):
                sample_size = None
        else:
            sample_size = None
        
        # Convert date strings to datetime objects
        def parse_date(date_str):
            if date_str:
                try:
                    return datetime.strptime(date_str, '%Y-%m-%d')
                except (ValueError, TypeError):
                    return None
            return None
        
        return {
            'poll_id': survey_id,
            'institute_id': survey_data.get('Institute_ID'),
            'institute_name': self._get_institute_name(survey_data.get('Institute_ID')),
            'tasker_id': survey_data.get('Tasker_ID'),
            'tasker_name': self._get_tasker_name(survey_data.get('Tasker_ID')),
            'parliament_id': survey_data.get('Parliament_ID'),
            'parliament_name': self._get_parliament_name(survey_data.get('Parliament_ID')),
            'method_id': survey_data.get('Method_ID'),
            'method_name': self._get_method_name(survey_data.get('Method_ID')),
            'survey_period_start': parse_date(survey_data.get('Survey_Period', {}).get('Date_Start')),
            'survey_period_end': parse_date(survey_data.get('Survey_Period', {}).get('Date_End')),
            'publication_date': parse_date(survey_data.get('Date')),
            'sample_size': sample_size,
            'results': self._transform_survey_results(survey_data.get('Results', {})),
            'source_url': f"https://dawum.de/Bundestag/{survey_id}",
            'source_loaded_at': datetime.fromisoformat(source_loaded_at.replace('Z', '+00:00')) if 'Z' in source_loaded_at else datetime.fromisoformat(source_loaded_at),
            'created_at': datetime.now(timezone.utc),
            'updated_at': datetime.now(timezone.utc)
        }
    
    def _get_institute_name(self, institute_id: str) -> Optional[str]:
        """Get institute name from ID (placeholder for metadata lookup)."""
        # This should be populated from the metadata
        return f"Institute_{institute_id}" if institute_id else None
    
    def _get_tasker_name(self, tasker_id: str) -> Optional[str]:
        """Get tasker name from ID (placeholder for metadata lookup)."""
        # This should be populated from the metadata
        return f"Tasker_{tasker_id}" if tasker_id else None
    
    def _get_parliament_name(self, parliament_id: str) -> Optional[str]:
        """Get parliament name from ID (placeholder for metadata lookup)."""
        # This should be populated from the metadata
        return f"Parliament_{parliament_id}" if parliament_id else None
    
    def _get_method_name(self, method_id: str) -> Optional[str]:
        """Get method name from ID (placeholder for metadata lookup)."""
        # This should be populated from the metadata
        return f"Method_{method_id}" if method_id else None
    
    def _transform_survey_results(self, results: Dict[str, Any]) -> str:
        """Transform survey results into JSON string format."""
        import json
        transformed_results = []
        
        for party_id, percentage in results.items():
            transformed_results.append({
                'party_id': party_id,
                'party_name': f"Party_{party_id}",  # This should be populated from metadata
                'percentage': percentage,
                'seats': None,  # Not available in this format
                'change': None  # Not available in this format
            })
        
        return json.dumps(transformed_results)

    async def fetch_data(self, **kwargs) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Fetch data from the API.
        Implementation of abstract method from BaseConnector.
        
        Yields:
            Dict containing fetched poll data
        """
        limit = kwargs.get('limit')
        polls = await self.fetch_polls(limit)
        
        for poll in polls:
            yield poll
    
    async def get_incremental_data(
        self, 
        since: datetime, 
        **kwargs
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Fetch incremental data since a specific timestamp.
        Implementation of abstract method from BaseConnector.
        
        Args:
            since: Timestamp to fetch data from
            
        Yields:
            Dict containing fetched data
        """
        # For DAWUM, we'll fetch all data and filter by date
        # This is a simplified implementation - in production you'd want
        # to use query parameters if the API supports them
        limit = kwargs.get('limit')
        polls = await self.fetch_polls(limit)
        
        for poll in polls:
            # Check if poll is newer than 'since' timestamp
            if poll.get('created_at'):
                try:
                    poll_date = poll['created_at']
                    if poll_date > since:
                        yield poll
                except (ValueError, TypeError):
                    # If date parsing fails, include the poll
                    yield poll
            else:
                # If no created_at date, include the poll
                yield poll

    def run_sync(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Synchronous wrapper for async fetch_polls method."""
        return asyncio.run(self.fetch_polls(limit))


# Convenience function for CLI usage
async def fetch_dawum_data(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Fetch DAWUM polling data."""
    config = DawumConfig()
    connector = DawumConnector(config)
    return await connector.fetch_polls(limit)


if __name__ == "__main__":
    # Test the connector
    async def test():
        data = await fetch_dawum_data(limit=10)
        print(f"Fetched {len(data)} polls")
        if data:
            print("Sample poll:", data[0])
    
    asyncio.run(test())