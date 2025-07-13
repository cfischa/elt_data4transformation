"""
DAWUM polling data connector.
Fetches polling data from DAWUM API with async HTTP, pagination, and retry logic.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
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
        
        async with session.get(url, params=params) as response:
            if response.status == 429:  # Rate limited
                retry_after = int(response.headers.get('Retry-After', 60))
                await asyncio.sleep(retry_after)
                raise aiohttp.ClientError("Rate limited")
            
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
        page = 1
        page_size = 100
        source_loaded_at = datetime.utcnow().isoformat()
        
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        connector = aiohttp.TCPConnector(limit=10)
        
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            while True:
                await self.rate_limiter.wait_if_needed()
                
                params = {
                    'page': page,
                    'limit': page_size
                }
                
                if self.config.api_key:
                    params['api_key'] = self.config.api_key
                
                try:
                    self.logger.info(f"Fetching DAWUM polls page {page}")
                    response_data = await self._fetch_page(session, 'polls', params)
                    
                    page_polls = response_data.get('data', [])
                    if not page_polls:
                        break
                    
                    # Transform and enrich poll data
                    for poll in page_polls:
                        transformed_poll = self._transform_poll(poll, source_loaded_at)
                        polls.append(transformed_poll)
                        
                        if limit and len(polls) >= limit:
                            break
                    
                    self.logger.info(f"Fetched {len(page_polls)} polls from page {page}")
                    
                    # Check if we've reached the limit or last page
                    if limit and len(polls) >= limit:
                        break
                    
                    if len(page_polls) < page_size:  # Last page
                        break
                        
                    page += 1
                    
                except Exception as e:
                    self.logger.error(f"Error fetching page {page}: {e}")
                    raise
        
        self.logger.info(f"Successfully fetched {len(polls)} total polls from DAWUM")
        return polls[:limit] if limit else polls
    
    def _transform_poll(self, poll: Dict[str, Any], source_loaded_at: str) -> Dict[str, Any]:
        """Transform raw poll data into standardized format."""
        return {
            'poll_id': poll.get('id'),
            'institute_id': poll.get('institute', {}).get('id'),
            'institute_name': poll.get('institute', {}).get('name'),
            'tasker_id': poll.get('tasker', {}).get('id'),
            'tasker_name': poll.get('tasker', {}).get('name'),
            'parliament_id': poll.get('parliament', {}).get('id'),
            'parliament_name': poll.get('parliament', {}).get('name'),
            'method_id': poll.get('method', {}).get('id'),
            'method_name': poll.get('method', {}).get('name'),
            'survey_period_start': self._parse_date(poll.get('survey_period', {}).get('start')),
            'survey_period_end': self._parse_date(poll.get('survey_period', {}).get('end')),
            'publication_date': self._parse_date(poll.get('publication_date')),
            'sample_size': poll.get('sample_size'),
            'results': self._transform_results(poll.get('results', [])),
            'source_url': poll.get('source_url'),
            'source_loaded_at': source_loaded_at,
            'created_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }
    
    def _transform_results(self, results: List[Dict[str, Any]]) -> str:
        """Transform poll results into JSON string format."""
        import json
        transformed_results = []
        
        for result in results:
            transformed_results.append({
                'party_id': result.get('party', {}).get('id'),
                'party_name': result.get('party', {}).get('name'),
                'party_short': result.get('party', {}).get('short'),
                'percentage': result.get('percentage'),
                'seats': result.get('seats'),
                'change': result.get('change')
            })
        
        return json.dumps(transformed_results)
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Parse and validate date strings."""
        if not date_str:
            return None
        
        try:
            # Try different date formats
            for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f']:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.isoformat()
                except ValueError:
                    continue
            
            # If no format matches, return as-is but log warning
            self.logger.warning(f"Could not parse date: {date_str}")
            return date_str
            
        except Exception as e:
            self.logger.error(f"Error parsing date {date_str}: {e}")
            return None
    
    async def fetch_metadata(self) -> Dict[str, Any]:
        """Fetch metadata about parties, institutes, methods etc."""
        metadata = {}
        
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for endpoint in ['parties', 'institutes', 'methods', 'parliaments', 'taskers']:
                try:
                    self.logger.info(f"Fetching DAWUM {endpoint}")
                    response_data = await self._fetch_page(session, endpoint, {})
                    metadata[endpoint] = response_data.get('data', [])
                except Exception as e:
                    self.logger.error(f"Error fetching {endpoint}: {e}")
                    metadata[endpoint] = []
        
        return metadata
    
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