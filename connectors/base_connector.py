"""
Base connector abstract class for all data source connectors.
Provides common functionality for API authentication, rate limiting, and error handling.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, AsyncGenerator
import aiohttp
import time
from datetime import datetime, timedelta
from pydantic import BaseModel, Field


class ConnectorConfig(BaseModel):
    """Base configuration for connectors."""
    api_key: Optional[str] = None
    base_url: str
    rate_limit_requests: int = 100
    rate_limit_period: int = 60  # seconds
    timeout: int = 30
    max_retries: int = 3
    retry_delay: int = 1


class RateLimiter:
    """Simple rate limiter for API calls."""
    
    def __init__(self, requests_per_period: int, period: int):
        self.requests_per_period = requests_per_period
        self.period = period
        self.calls = []
    
    async def wait_if_needed(self) -> None:
        """Wait if we've exceeded the rate limit."""
        now = time.time()
        
        # Remove old calls outside the period
        self.calls = [call_time for call_time in self.calls 
                     if now - call_time < self.period]
        
        # Check if we need to wait
        if len(self.calls) >= self.requests_per_period:
            oldest_call = min(self.calls)
            wait_time = self.period - (now - oldest_call)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
        
        # Record this call
        self.calls.append(now)


class BaseConnector(ABC):
    """
    Abstract base class for all data source connectors.
    Provides common functionality for API calls, rate limiting, and error handling.
    """
    
    def __init__(self, config: ConnectorConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.rate_limiter = RateLimiter(
            config.rate_limit_requests,
            config.rate_limit_period
        )
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.timeout)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def _make_request(
        self, 
        method: str, 
        url: str, 
        **kwargs
    ) -> Dict[str, Any]:
        """
        Make an HTTP request with rate limiting and retries.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            **kwargs: Additional arguments for aiohttp request
            
        Returns:
            Response data as dictionary
            
        Raises:
            Exception: If request fails after max retries
        """
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")
        
        for attempt in range(self.config.max_retries + 1):
            try:
                # Wait for rate limiter
                await self.rate_limiter.wait_if_needed()
                
                self.logger.debug(f"Making {method} request to {url} (attempt {attempt + 1})")
                
                # Add authentication headers
                headers = kwargs.get('headers', {})
                if self.config.api_key:
                    headers.update(self._get_auth_headers())
                kwargs['headers'] = headers
                
                async with self.session.request(method, url, **kwargs) as response:
                    response.raise_for_status()
                    
                    # Handle different content types
                    content_type = response.headers.get('content-type', '')
                    if 'application/json' in content_type:
                        return await response.json()
                    else:
                        text = await response.text()
                        return {'content': text}
                    
            except aiohttp.ClientError as e:
                self.logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
                
                if attempt < self.config.max_retries:
                    wait_time = self.config.retry_delay * (2 ** attempt)
                    await asyncio.sleep(wait_time)
                else:
                    raise
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """
        Get authentication headers for API requests.
        Override in subclasses for specific auth methods.
        """
        if self.config.api_key:
            return {'Authorization': f'Bearer {self.config.api_key}'}
        return {}
    
    async def get(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make GET request to API endpoint."""
        url = f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        return await self._make_request('GET', url, **kwargs)
    
    async def post(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make POST request to API endpoint."""
        url = f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        return await self._make_request('POST', url, **kwargs)
    
    @abstractmethod
    async def fetch_data(self, **kwargs) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Fetch data from the API.
        Must be implemented by subclasses.
        
        Yields:
            Dict containing fetched data
        """
        pass
    
    @abstractmethod
    async def get_incremental_data(
        self, 
        since: datetime, 
        **kwargs
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Fetch incremental data since a specific timestamp.
        Must be implemented by subclasses.
        
        Args:
            since: Timestamp to fetch data from
            
        Yields:
            Dict containing fetched data
        """
        pass
    
    async def paginate(
        self, 
        endpoint: str, 
        page_size: int = 100,
        **kwargs
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Generic pagination helper.
        Override in subclasses for specific pagination schemes.
        
        Args:
            endpoint: API endpoint to paginate
            page_size: Number of items per page
            **kwargs: Additional parameters for the request
            
        Yields:
            Dict containing page data
        """
        page = 1
        
        while True:
            params = kwargs.get('params', {})
            params.update({
                'page': page,
                'per_page': page_size
            })
            kwargs['params'] = params
            
            response = await self.get(endpoint, **kwargs)
            
            # Generic handling - override in subclasses
            if 'data' in response:
                data = response['data']
                if not data:
                    break
                yield response
                page += 1
            else:
                yield response
                break
    
    def validate_config(self) -> None:
        """Validate connector configuration."""
        if not self.config.base_url:
            raise ValueError("base_url is required")
        
        if self.config.rate_limit_requests <= 0:
            raise ValueError("rate_limit_requests must be positive")
        
        if self.config.rate_limit_period <= 0:
            raise ValueError("rate_limit_period must be positive")
    
    def get_last_sync_time(self) -> Optional[datetime]:
        """
        Get the last synchronization time from persistent storage.
        Override in subclasses to implement persistence.
        """
        # TODO: Implement persistent storage for sync times
        return None
    
    def set_last_sync_time(self, timestamp: datetime) -> None:
        """
        Set the last synchronization time in persistent storage.
        Override in subclasses to implement persistence.
        """
        # TODO: Implement persistent storage for sync times
        pass
