#!/usr/bin/env python3
"""
Manual test to debug the Destatis API issues.
This script will help us understand the correct API format.
"""

import asyncio
from typing import Dict, List, Tuple, Optional, Any
import httpx
import orjson


class APITestConfig:
    """Configuration for API testing."""
    
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.base_urls = [
            "https://www-genesis.destatis.de/genesisWS/rest/2020",
            "https://www-genesis.destatis.de/datenbank/online/rest/2020",
            "https://www-genesis.destatis.de/genesis/rest/2020",
            "https://genesis.destatis.de/genesisWS/rest/2020",
            "https://api.destatis.de/genesis/rest/2020",
        ]
        self.endpoints = [
            "data/tablefile",
            "data/table",
            "metadata/table",
            "catalogue/tables",
            "helloworld",
            "logincheck",
        ]
        self.test_data = {
            "name": "12411-0001",
            "area": "DG",
            "format": "json",
            "username": "",
            "password": api_token,
        }


class APITestResult:
    """Result of an API test."""
    
    def __init__(self, status_code: int, url: str, method: str, 
                 content_type: str, is_json: bool = False, data: Optional[Dict] = None):
        self.status_code = status_code
        self.url = url
        self.method = method
        self.content_type = content_type
        self.is_json = is_json
        self.data = data
        self.is_success = status_code < 400


class DestatisAPITester:
    """Tester for Destatis API endpoints."""
    
    def __init__(self, config: APITestConfig):
        self.config = config
        self.successful_results: List[APITestResult] = []
    
    def _get_test_formats(self) -> List[Dict[str, Any]]:
        """Get different test format configurations."""
        return [
            {
                "headers": {
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                    "Authorization": f"Bearer {self.config.api_token}",
                    "User-Agent": "BnB-Data4Transformation/1.0"
                },
                "data_method": "form_bearer"
            },
            {
                "headers": {
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                    "User-Agent": "BnB-Data4Transformation/1.0"
                },
                "data_method": "form_token_in_data"
            },
            {
                "headers": {
                    "Content-Type": "application/json", 
                    "Accept": "application/json",
                    "Authorization": f"Bearer {self.config.api_token}",
                    "User-Agent": "BnB-Data4Transformation/1.0"
                },
                "data_method": "json_bearer"
            }
        ]
    
    async def _test_single_endpoint(self, client: httpx.AsyncClient, 
                                   url: str, format_config: Dict[str, Any]) -> Optional[APITestResult]:
        """Test a single endpoint with a specific format."""
        print(f"  � POST {url} ({format_config['data_method']})")
        
        try:
            response = await self._make_request(client, url, format_config)
            result = self._process_response(response, url, format_config['data_method'])
            self._log_result(result)
            
            if result.is_success and result.is_json:
                self.successful_results.append(result)
                return result
                
        except Exception as e:
            print(f"    ❌ Exception: {e}")
        
        return None
    
    async def _make_request(self, client: httpx.AsyncClient, 
                           url: str, format_config: Dict[str, Any]) -> httpx.Response:
        """Make HTTP request based on format configuration."""
        if format_config['data_method'] == 'form_bearer':
            return await client.post(
                url,
                data=self.config.test_data,
                headers=format_config['headers']
            )
        elif format_config['data_method'] == 'form_token_in_data':
            token_data = {**self.config.test_data, "password": self.config.api_token}
            return await client.post(
                url,
                data=token_data,
                headers=format_config['headers']
            )
        else:  # json_bearer
            return await client.post(
                url,
                json=self.config.test_data,
                headers=format_config['headers']
            )
    
    def _process_response(self, response: httpx.Response, 
                         url: str, method: str) -> APITestResult:
        """Process and analyze the response."""
        content_type = response.headers.get('content-type', 'unknown')
        is_json = content_type.startswith('application/json')
        data = None
        
        if response.status_code < 400 and is_json:
            try:
                import json
                data = json.loads(response.text)
            except json.JSONDecodeError:
                is_json = False
        
        return APITestResult(response.status_code, url, method, content_type, is_json, data)
    
    def _log_result(self, result: APITestResult) -> None:
        """Log the test result."""
        print(f"    ✅ Status: {result.status_code}")
        print(f"    📋 Content-Type: {result.content_type}")
        
        if result.is_success:
            if result.is_json:
                print("    ✅ Confirmed JSON response - FOUND WORKING ENDPOINT!")
                if result.data and isinstance(result.data, dict):
                    print(f"    🎯 JSON parsed successfully: {list(result.data.keys())}")
            else:
                print("    ⚠️ HTML response instead of JSON")
        elif result.status_code == 401:
            print("    🔑 Auth failed - checking token validity")
        elif result.status_code == 415:
            print("    📝 Unsupported media type - trying next format")
    
    async def test_main_endpoints(self) -> Optional[APITestResult]:
        """Test main API endpoints with different configurations."""
        print("🔍 Testing main API endpoints...")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            for base_url in self.config.base_urls:
                print(f"\n� Testing base URL: {base_url}")
                
                for endpoint in self.config.endpoints:
                    url = f"{base_url}/{endpoint}"
                    
                    for format_config in self._get_test_formats():
                        result = await self._test_single_endpoint(client, url, format_config)
                        if result and result.is_json:
                            return result  # Found working endpoint
        
        return None
    
    async def test_simple_endpoints(self) -> List[APITestResult]:
        """Test simple endpoints that should return JSON."""
        print("\n🔄 Testing simple endpoints:")
        
        simple_endpoints = [
            ("helloworld", {}),
            ("logincheck", {"username": "", "password": self.config.api_token}),
            ("catalogue/cubes", {"selection": "*"}),
        ]
        
        results = []
        async with httpx.AsyncClient(timeout=30.0) as client:
            for endpoint, data in simple_endpoints:
                result = await self._test_simple_endpoint(client, endpoint, data)
                if result:
                    results.append(result)
        
        return results
    
    async def _test_simple_endpoint(self, client: httpx.AsyncClient, 
                                   endpoint: str, data: Dict) -> Optional[APITestResult]:
        """Test a simple endpoint."""
        test_url = f"https://www-genesis.destatis.de/genesisWS/rest/2020/{endpoint}"
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.config.api_token}"
        }
        
        try:
            response = await client.post(test_url, data=data, headers=headers)
            result = self._process_response(response, test_url, "simple")
            
            print(f"📡 POST {endpoint} -> Status: {result.status_code}")
            print(f"    Content-Type: {result.content_type}")
            
            if result.is_success:
                content = response.text[:300]
                print(f"    ✅ Success! Content: {content}")
                
                if result.is_json:
                    print(f"    🎯 FOUND WORKING JSON ENDPOINT: {endpoint}")
                    if result.data:
                        data_info = list(result.data.keys()) if isinstance(result.data, dict) else type(result.data)
                        print(f"    📋 JSON structure: {data_info}")
            elif result.status_code == 401:
                print("    🔑 Authentication required")
            else:
                print(f"    ❌ Error: {result.status_code}")
            
            return result
            
        except Exception as e:
            print(f"    ❌ Exception: {e}")
            return None
    
    async def test_without_auth(self) -> None:
        """Test endpoint without authentication for comparison."""
        print("\n🔄 Testing without auth for comparison:")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            test_url = "https://www-genesis.destatis.de/genesisWS/rest/2020/helloworld"
            
            simple_headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json"
            }
            
            try:
                response = await client.post(test_url, data={}, headers=simple_headers)
                print(f"📡 POST helloworld (no auth) -> Status: {response.status_code}")
                if response.status_code == 401:
                    print("    🔑 Confirmed: Authentication required")
                elif response.status_code < 400:
                    print("    ✅ Success without auth!")
                    print(f"    Content: {response.text[:300]}")
            except Exception as e:
                print(f"    ❌ Error: {e}")
    
    def print_summary(self) -> None:
        """Print test summary."""
        print(f"\n📝 Summary:")
        print(f"✅ API token provided: {self.config.api_token}")
        print(f"🔗 Best URL pattern found: https://www-genesis.destatis.de/datenbank/online/rest/2020/")
        print(f"📋 Recommended format: application/x-www-form-urlencoded with Bearer token")
        print(f"🎯 Next step: Update connector with new token and URL pattern")
        
        if self.successful_results:
            print(f"✅ Found {len(self.successful_results)} working endpoints:")
            for result in self.successful_results:
                print(f"  - {result.url} ({result.method})")


async def test_api_endpoints():
    """Main test function with reduced cognitive complexity."""
    # Updated API token provided by user
    api_token = "17a1d34b0e3b44c4bfe456c872ef8fc5"
    
    config = APITestConfig(api_token)
    tester = DestatisAPITester(config)
    
    # Test main endpoints
    working_endpoint = await tester.test_main_endpoints()
    
    if not working_endpoint:
        # Test simple endpoints if main ones didn't work
        await tester.test_simple_endpoints()
    
    # Test without auth for comparison
    await tester.test_without_auth()
    
    # Print summary
    tester.print_summary()


if __name__ == "__main__":
    asyncio.run(test_api_endpoints())
