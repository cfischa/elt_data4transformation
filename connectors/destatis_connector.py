"""
GENESIS REST API (Destatis) connector.
Fetches statistical data from German Federal Statistical Office with async HTTP,
chunking, pagination, and retry logic.
"""

import asyncio
import base64
import gzip
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, AsyncGenerator
from urllib.parse import urljoin
import tempfile

import aiofiles
import httpx
import orjson
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from pydantic import BaseModel, Field

from .base_connector import BaseConnector, ConnectorConfig
from elt.metadata import RawIngestion, DataSource, IngestionStatus
from elt.utils.logging_config import get_logger
from elt.utils.persistence import PersistenceManager

# GENESIS REST base URL (Using 2020 API version)
BASE_URL = "https://www-genesis.destatis.de/genesisWS/rest/2020"

# API endpoints (Official 2025 REST API)
ENDPOINT_CATALOGUE_CUBES = "catalogue/cubes"        # Metadata discovery (primary)
ENDPOINT_CATALOGUE_TABLES = "catalogue/tables"      # Tables listing (fallback/alt)
ENDPOINT_DATA_CUBEFILE = "data/cubefile"            # Raw cube data extraction
ENDPOINT_DATA_TABLEFILE = "data/tablefile"          # Table data extraction (fallback)
ENDPOINT_HELLOWORLD_LOGINCHECK = "helloworld/logincheck"  # Auth verification

# Legacy endpoints (deprecated but kept for fallback)
ENDPOINT_DATA_TABLE = "data/table"                  # Legacy table endpoint
ENDPOINT_METADATA_TABLE = "metadata/table"          # Legacy metadata endpoint


class DestatisInvalidContent(Exception):
    """Raised when API returns non-JSON content."""
    pass


class DestatisConfig(ConnectorConfig):
    """Configuration for GENESIS-Online REST API connector."""
    
    base_url: str = BASE_URL
    username: Optional[str] = None
    password: Optional[str] = None
    api_token: Optional[str] = None  # API token instead of username/password
    rate_limit_requests: int = 30
    rate_limit_period: int = 60
    timeout: int = 180
    max_retries: int = 3
    chunk_size_mb: int = 10
    max_cells_per_request: int = 1000000

    def __init__(self, **kwargs):
        import os
        from dotenv import load_dotenv
        
        # Load environment variables from .env file
        load_dotenv()
        
        # Load from env if not provided
        username = kwargs.get('username') or os.getenv('DESTATIS_USER')
        password = kwargs.get('password') or os.getenv('DESTATIS_PASS')
        api_token = kwargs.get('api_token') or os.getenv('DESTATIS_API_KEY') or os.getenv('DESTATIS_TOKEN')
        # Pass all to parent
        super().__init__(
            base_url=kwargs.get('base_url', BASE_URL),
            username=username,
            password=password,
            api_token=api_token,
            rate_limit_requests=kwargs.get('rate_limit_requests', 30),
            rate_limit_period=kwargs.get('rate_limit_period', 60),
            timeout=kwargs.get('timeout', 120),
            max_retries=kwargs.get('max_retries', 3),
            chunk_size_mb=kwargs.get('chunk_size_mb', 10),
            max_cells_per_request=kwargs.get('max_cells_per_request', 1000000)
        )


class CubeInfo(BaseModel):
    """Information about a statistical cube from catalogue/cubes."""
    
    code: str                                    # Cube code (e.g., "61221BJ002")
    content: Optional[str] = None               # Human-readable title
    state: Optional[str] = None                 # Status (e.g., "vollständig mit Werten")
    time_coverage: Optional[str] = None         # Time span information
    latest_update: Optional[datetime] = None    # Last update timestamp
    information: Optional[bool] = None          # Information flag
    size_estimate: Optional[int] = None         # Estimated data size


class TableInfo(BaseModel):
    """Information about a statistical table (legacy/fallback)."""
    
    name: str
    description: Optional[str] = None
    updated: Optional[datetime] = None
    variables: Optional[List[str]] = None
    size_estimate: Optional[int] = None


class DestatisConnector(BaseConnector):
    """
    GENESIS-Online REST API connector for German Federal Statistical Office.
    
    Supports:
    - Async HTTP requests with proper authentication
    - Auto-chunking of large tables by time periods or cell count
    - Multiple formats (JSON-stat, SDMX-JSON, CSV fallback)
    - Transparent compression handling (gzip)
    - Exponential backoff retry logic
    - Structured metadata logging
    """
    
    def __init__(self, config: Optional[DestatisConfig] = None):
        self.config = config or DestatisConfig()
        super().__init__(self.config)
        self.logger = get_logger(self.__class__.__name__)
        self.persistence = PersistenceManager(base_path="./data/raw/destatis")
        self._client: Optional[httpx.AsyncClient] = None
    
    def _build_url(self, path: str) -> str:
        """Build URL from base endpoint and path."""
        base = (self.config.base_url or BASE_URL).rstrip('/')
        return f"{base}/{path.lstrip('/')}"
    
    async def __aenter__(self):
        """Async context manager entry."""
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.config.timeout),
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._client:
            await self._client.aclose()
    
    def _get_auth_params(self) -> Dict[str, str]:
        """Get authentication parameters for query string."""
        if self.config.api_token:
            # For token auth: token as username, empty password
            return {"username": self.config.api_token, "password": ""}
        elif self.config.username and self.config.password:
            return {"username": self.config.username, "password": self.config.password}
        return {}
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers (include legacy username/password header fields)."""
        headers: Dict[str, str] = {}
        if self.config.api_token:
            headers["Authorization"] = f"Bearer {self.config.api_token}"
            # headers["X-API-Key"] = self.config.api_token  # Not consistently used by GENESIS
            
            # CRITICAL: Official API requires token as username AND empty password header
            headers["username"] = self.config.api_token
            headers["password"] = ""
        elif self.config.username and self.config.password:
            credentials = f"{self.config.username}:{self.config.password}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            headers["Authorization"] = f"Basic {encoded_credentials}"
            headers["username"] = self.config.username
            headers["password"] = self.config.password
        return headers
    
    def _prepare_request_headers(self) -> Dict[str, str]:
        """Prepare HTTP headers for API request."""
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "*/*"
        }
        # Add all auth-related headers (Bearer and legacy username/password)
        headers.update(self._get_auth_headers())
        return headers
    
    def _prepare_form_data(
        self, 
        endpoint: str, 
        data: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Prepare form data for API request."""
        form_data = data.copy() if data else {}
        
        # Provide credentials in form data (required by GENESIS REST)
        if self.config.api_token:
            form_data.setdefault("username", self.config.api_token)
            form_data.setdefault("password", "")
        elif self.config.username and self.config.password:
            form_data.setdefault("username", self.config.username)
            form_data.setdefault("password", self.config.password)
        
        # Force JSON format unless overridden
        if "format" not in form_data:
            form_data["format"] = "JSON"
        # Default language if not provided
        form_data.setdefault("language", "de")
        
        # Add mandatory area parameter if not present
        if endpoint == ENDPOINT_DATA_TABLE and "area" not in form_data:
            form_data["area"] = "all"
        
        return form_data
    
    async def _handle_response_errors(self, response: httpx.Response) -> None:
        """Handle HTTP response errors with appropriate retry logic."""
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            self.logger.warning(f"Rate limited, waiting {retry_after} seconds")
            await asyncio.sleep(retry_after)
            raise httpx.HTTPStatusError(
                "Rate limited", request=response.request, response=response
            )
        
        if response.status_code >= 500:
            self.logger.warning(f"Server error {response.status_code}, retrying")
            raise httpx.HTTPStatusError(
                f"Server error {response.status_code}", 
                request=response.request, 
                response=response
            )
        
        response.raise_for_status()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TimeoutException))
    )
    async def _make_request(
        self, 
        endpoint: str, 
        data: Optional[Dict[str, Any]] = None,
        expect_json: bool = True,
    ) -> httpx.Response:
        """Make HTTP POST request with retry logic (new API requirement since July 2025)."""
        if not self._client:
            raise RuntimeError("Client not initialized. Use async context manager.")
        
        url = self._build_url(endpoint)
        headers = self._prepare_request_headers()
        
        # Rate limiting
        await self.rate_limiter.wait_if_needed()
        
        self.logger.debug(f"Making POST request to {url} with data {data}")
        
        form_data = self._prepare_form_data(endpoint, data)
        
        response = await self._client.post(url, data=form_data, headers=headers)
        
        # Handle response errors
        await self._handle_response_errors(response)
        
        if expect_json:
            self._validate_response_content(response)
        
        return response
    
    def _validate_response_content(self, response: httpx.Response) -> None:
        """Validate that response contains JSON content."""
        content_type = response.headers.get('content-type', '')
        response_text = response.text.lstrip()
        
        if not (content_type.startswith('application/json') or response_text.startswith('{')):
            raise DestatisInvalidContent("HTML or unknown response")
    
    def _validate_json_shape(self, data: Dict[str, Any], endpoint: str) -> None:
        """Light JSON shape sanity check for API responses."""
        expected_keys = {"Identifier", "Status", "Object"}
        
        if endpoint == ENDPOINT_DATA_TABLE:
            actual_keys = set(data.keys())
            missing_keys = expected_keys - actual_keys
            
            if missing_keys:
                self.logger.warning(f"Missing expected keys in response: {missing_keys}. Got keys: {actual_keys}")
            else:
                self.logger.debug("Response shape validation passed for data/table endpoint")
    
    async def get_table_info(self, table_id: str) -> TableInfo:
        """Get metadata information about a table."""
        response = await self._make_request(ENDPOINT_METADATA_TABLE, {
            "name": table_id
        })
        data = response.json()
        
        # Parse response to extract table info
        table_data = data.get("Object", {})
        
        return TableInfo(
            name=table_data.get("Code", table_id),
            description=table_data.get("Content", ""),
            updated=self._parse_datetime(table_data.get("Updated")),
            variables=table_data.get("Variable", []),
            size_estimate=table_data.get("Size")
        )
    
    def _parse_datetime(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string from GENESIS API."""
        if not date_str:
            return None
        
        try:
            # Try multiple formats that GENESIS API uses
            formats = [
                "%d.%m.%Y %H:%M:%S",     # Standard format: "16.06.2025 13:06:35"  
                "%d.%m.%Y %H:%M:%Sh",    # Format with 'h' suffix: "16.06.2025 13:06:35h"
            ]
            
            # Remove 'h' suffix if present
            clean_date_str = date_str.rstrip('h')
            
            for fmt in formats:
                try:
                    return datetime.strptime(clean_date_str, fmt)
                except ValueError:
                    continue
                    
            # If no format worked, log the exact string for debugging
            self.logger.warning(f"Could not parse datetime format: '{date_str}' (cleaned: '{clean_date_str}')")
            return None
            
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Could not parse datetime: {date_str} - {e}")
            return None
    
    def _calculate_chunks(
        self, 
        start_year: Optional[int], 
        end_year: Optional[int],
        estimated_size: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Calculate optimal chunks for large tables."""
        chunks = []
        
        if not start_year or not end_year:
            # Single chunk if no year range specified
            return [{}]
        
        # If we have size estimate, calculate chunks based on size
        if estimated_size and estimated_size > self.config.max_cells_per_request:
            years_per_chunk = max(1, (end_year - start_year + 1) // 
                                 math.ceil(estimated_size / self.config.max_cells_per_request))
        else:
            # Default to 10-year chunks for large time spans
            years_per_chunk = 10
        
        current_year = start_year
        while current_year <= end_year:
            chunk_end = min(current_year + years_per_chunk - 1, end_year)
            chunks.append({
                "startYear": str(current_year),
                "endYear": str(chunk_end)
            })
            current_year = chunk_end + 1
        
        return chunks
    
    async def _extract_table_chunks(
        self,
        table_id: str,
        chunks: List[Dict[str, Any]],
        area: str,
        fmt: str
    ) -> tuple[List[Any], int]:
        """Extract data for all chunks of a table."""
        all_data = []
        total_records = 0
        
        for i, chunk_params in enumerate(chunks):
            self.logger.info(f"Fetching chunk {i+1}/{len(chunks)} for table {table_id}")
            
            # Prepare request payload for POST
            request_data = self._build_request_data(table_id, area, fmt, chunk_params)
            
            # Make POST request
            response = await self._make_request(
                ENDPOINT_DATA_TABLE,
                request_data,
                expect_json=(fmt.lower() == "json"),
            )
            
            # Process response
            chunk_data, records_count = await self._process_response(response, fmt)
            all_data.append(chunk_data)
            total_records += records_count
        
        return all_data, total_records
    
    def _build_request_data(
        self, 
        table_id: str, 
        area: str, 
        fmt: str, 
        chunk_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build request data for API call."""
        request_data = {
            "name": table_id,
            "area": area,
            "format": fmt,
            "compress": "true"
        }
        request_data.update(chunk_params)
        
        # Authentication is handled in _make_request via _get_auth_params()
        # No need to add it here as it gets added in the POST form data
        
        return request_data
    
    async def _process_response(
        self, 
        response: httpx.Response, 
        fmt: str
    ) -> tuple[Any, int]:
        """Process API response and extract data."""
        # Handle compressed response
        content = response.content
        if response.headers.get("content-encoding") == "gzip":
            content = gzip.decompress(content)
        
        records_count = 0
        
        # Parse response based on format
        if fmt == "json":
            chunk_data = orjson.loads(content)
            
            # Validate JSON shape for data endpoints
            self._validate_json_shape(chunk_data, ENDPOINT_DATA_TABLE)
            
            # Count records (approximate)
            if isinstance(chunk_data, dict):
                dataset = chunk_data.get("dataset", {})
                if "value" in dataset:
                    records_count = len(dataset["value"])
        elif fmt == "csv":
            # For CSV, decode to string
            chunk_data = content.decode("utf-8")
            # Count lines as approximate record count
            records_count = len(chunk_data.split('\n')) - 1  # Subtract header
        else:
            # SDMX or other formats
            chunk_data = orjson.loads(content)
        
        return chunk_data, records_count

    async def fetch_cube(
        self,
        cube_code: str,
        language: str = "de",
        format_type: str = "raw"
    ) -> Path:
        """
        Fetch raw cube data using the official data/cubefile endpoint.
        
        This is the recommended approach for getting complete multidimensional
        data with preserved statistical structure.
        
        Args:
            cube_code: Cube code from get_available_cubes (e.g., "61221BJ002")
            language: Language for labels ("de" or "en")
            format_type: Data format ("raw" for cube structure)
            
        Returns:
            Path to the saved raw cube data file
        """
        ingestion_start = datetime.now()
        
        self.logger.info(f"Starting cube extraction for {cube_code}")
        
        try:
            request_data = {
                "name": cube_code,
                "language": language
            }
            
            # Make request to cube endpoint
            response = await self._make_request(
                ENDPOINT_DATA_CUBEFILE,
                request_data,
                expect_json=False,
            )
            
            # Process and save cube data
            output_path = await self._save_cube_data(cube_code, response, language)
            
            # Log successful extraction
            file_size = output_path.stat().st_size if output_path.exists() else 0
            self.logger.info(
                f"Successfully extracted cube {cube_code}: "
                f"saved to {output_path} ({file_size:,} bytes)"
            )
            
            return output_path
            
        except Exception as e:
            self.logger.error(f"Failed to fetch cube {cube_code}: {e}")
            raise

    async def _save_cube_data(
        self, 
        cube_code: str, 
        response: httpx.Response,
        language: str
    ) -> Path:
        """Save raw cube data to storage."""
        # Handle compression if present
        content = response.content
        if response.headers.get("content-encoding") == "gzip":
            content = gzip.decompress(content)
        
        # Create output path
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"destatis_cube_{cube_code}_{language}_{timestamp}.txt"
        output_path = Path(tempfile.gettempdir()) / filename
        
        # Save raw cube data
        async with aiofiles.open(output_path, 'wb') as f:
            await f.write(content)
        
        return output_path

    async def fetch_table(
        self,
        table_id: str,
        area: str = "de",
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        fmt: str = "json"
    ) -> Path:
        """
        Fetch a complete statistical table, auto-chunking if necessary.
        
        Args:
            table_id: GENESIS table identifier (e.g., "12411-0001")
            area: Geographic area code (default: "de" for Germany)
            start_year: Start year for time series data
            end_year: End year for time series data  
            fmt: Data format ("json", "sdmx", "csv")
            
        Returns:
            Path to the saved raw data file in local storage
        """
        ingestion_start = datetime.now()
        
        # Log extraction start
        self.logger.info(f"Starting extraction for table {table_id}")
        
        try:
            # Get table metadata
            table_info = await self.get_table_info(table_id)
            
            # Calculate chunks
            chunks = self._calculate_chunks(start_year, end_year, table_info.size_estimate)
            
            # Extract all chunks
            all_data, total_records = await self._extract_table_chunks(
                table_id, chunks, area, fmt
            )
            
            # Combine chunks and save
            output_path = await self._save_combined_data(
                table_id, all_data, fmt, area, start_year, end_year
            )
            
            # Store successful ingestion record
            await self._store_ingestion_record(
                table_id, ingestion_start, total_records, output_path, 
                table_info, area, start_year, end_year, fmt, len(chunks)
            )
            
            self.logger.info(
                f"Successfully extracted table {table_id}: "
                f"{total_records} records in {len(chunks)} chunks"
            )
            
            return output_path
            
        except Exception as e:
            self.logger.error(f"Failed to extract table {table_id}: {e}")
            
            # Store failed ingestion record
            await self._store_failed_ingestion_record(
                table_id, ingestion_start, str(e), area, start_year, end_year, fmt
            )
            
            raise
    
    async def _store_ingestion_record(
        self,
        table_id: str,
        ingestion_start: datetime,
        total_records: int,
        output_path: Path,
        table_info: TableInfo,
        area: str,
        start_year: Optional[int],
        end_year: Optional[int],
        fmt: str,
        chunks_count: int
    ) -> None:
        """Store successful ingestion record in metadata database."""
        ingestion = RawIngestion(
            source=DataSource.DESTATIS,
            dataset=table_id,
            timestamp=ingestion_start,
            records_count=total_records,
            file_path=str(output_path),
            status=IngestionStatus.SUCCESS,
            metadata={
                "table_info": table_info.dict(),
                "area": area,
                "start_year": start_year,
                "end_year": end_year,
                "format": fmt,
                "chunks_count": chunks_count,
                "duration_seconds": (datetime.now() - ingestion_start).total_seconds()
            }
        )
        # Implementation: Store to ClickHouse metadata table
        # For now, log the ingestion record
        self.logger.info(f"Ingestion record: {ingestion.dict()}")
    
    async def _store_failed_ingestion_record(
        self,
        table_id: str,
        ingestion_start: datetime,
        error_message: str,
        area: str,
        start_year: Optional[int],
        end_year: Optional[int],
        fmt: str
    ) -> None:
        """Store failed ingestion record in metadata database."""
        failed_ingestion = RawIngestion(
            source=DataSource.DESTATIS,
            dataset=table_id,
            timestamp=ingestion_start,
            records_count=0,
            status=IngestionStatus.FAILED,
            error_message=error_message,
            metadata={
                "area": area,
                "start_year": start_year,
                "end_year": end_year,
                "format": fmt,
            }
        )
        # Implementation: Store to ClickHouse metadata table
        # For now, log the failed ingestion record
        self.logger.error(f"Failed ingestion record: {failed_ingestion.dict()}")
    
    async def _save_combined_data(
        self,
        table_id: str,
        all_data: List[Any],
        fmt: str,
        area: str,
        start_year: Optional[int],
        end_year: Optional[int]
    ) -> Path:
        """Save combined chunk data to file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generate filename
        year_suffix = ""
        if start_year and end_year:
            year_suffix = f"_{start_year}_{end_year}"
        
        filename = f"{table_id}_{area}{year_suffix}_{timestamp}"
        
        if fmt == "json":
            # Combine JSON chunks
            if len(all_data) == 1:
                combined_data = all_data[0]
            else:
                # Merge multiple JSON-stat datasets
                combined_data = self._merge_jsonstat_datasets(all_data)
            
            output_path = self.persistence.save_json(combined_data, filename, "tables")
            
        elif fmt == "csv":
            # Combine CSV chunks
            combined_csv = self._merge_csv_chunks(all_data)
            output_path = self.persistence.base_path / "tables" / f"{filename}.csv"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            async with aiofiles.open(output_path, 'w', encoding='utf-8') as f:
                await f.write(combined_csv)
                
        else:
            # SDMX or other formats - save as JSON
            output_path = self.persistence.save_json(all_data, filename, "tables")
        
        self.logger.info(f"Saved combined data to {output_path}")
        return output_path
    
    def _merge_jsonstat_datasets(self, datasets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Merge multiple JSON-stat datasets into one."""
        if not datasets:
            return {}
        
        # Start with first dataset
        merged = datasets[0].copy()
        
        if len(datasets) == 1:
            return merged
        
        # Merge additional datasets
        merged_dataset = merged.get("dataset", {})
        merged_values = merged_dataset.get("value", {})
        
        for dataset in datasets[1:]:
            dataset_data = dataset.get("dataset", {})
            values = dataset_data.get("value", {})
            
            # Merge values (simple concatenation for now)
            if isinstance(merged_values, dict) and isinstance(values, dict):
                merged_values.update(values)
            elif isinstance(merged_values, list) and isinstance(values, list):
                merged_values.extend(values)
        
        merged_dataset["value"] = merged_values
        merged["dataset"] = merged_dataset
        
        return merged
    
    def _merge_csv_chunks(self, csv_chunks: List[str]) -> str:
        """Merge CSV chunks, preserving header from first chunk."""
        if not csv_chunks:
            return ""
        
        if len(csv_chunks) == 1:
            return csv_chunks[0]
        
        # Split first chunk to get header
        lines = csv_chunks[0].strip().split('\n')
        header = lines[0] if lines else ""
        first_data = '\n'.join(lines[1:]) if len(lines) > 1 else ""
        
        # Combine all data (skip headers from subsequent chunks)
        all_data = [first_data]
        
        for chunk in csv_chunks[1:]:
            lines = chunk.strip().split('\n')
            data = '\n'.join(lines[1:]) if len(lines) > 1 else ""
            if data:
                all_data.append(data)
        
        # Combine header with all data
        return header + '\n' + '\n'.join(filter(None, all_data))
    
    async def get_available_cubes(
        self, 
        selection: str = "*", 
        pagelength: int = 50000,
        language: str = "de"
    ) -> List[CubeInfo]:
        """
        Get list of available statistical cubes (OFFICIAL METADATA-FIRST APPROACH).
        
        This is the primary method for metadata discovery using the official
        catalogue/cubes endpoint as specified in the 2025 API documentation.
        
        Args:
            selection: Search pattern (e.g., "*" for all, "61*" for demographic data)
            pagelength: Maximum number of cubes to return per request
            language: Language for descriptions ("de" or "en")
            
        Returns:
            List of CubeInfo objects with metadata about available cubes
        """
        request_data = {
            "selection": selection,
            "pagelength": str(pagelength),
            "language": language
        }
        
        self.logger.info(f"Discovering cubes with selection pattern: {selection}")
        
        try:
            response = await self._make_request(ENDPOINT_CATALOGUE_CUBES, request_data)
            data = response.json()
            
            cubes = []
            cube_list = data.get("List") or []
            
            self.logger.info(f"Found {len(cube_list)} cubes matching pattern '{selection}'")
            
            for cube_data in cube_list:
                cube_info = CubeInfo(
                    code=cube_data.get("Code", ""),
                    content=cube_data.get("Content", ""),
                    state=cube_data.get("State", ""),
                    time_coverage=cube_data.get("Time", ""),
                    latest_update=self._parse_datetime(cube_data.get("LatestUpdate")),
                    information=cube_data.get("Information", False),
                    size_estimate=None  # Will be estimated based on time coverage
                )
                cubes.append(cube_info)
            
            # Sort by latest update (newest first) for prioritization
            cubes.sort(key=lambda x: x.latest_update or datetime.min, reverse=True)
            
            self.logger.info(f"Successfully parsed {len(cubes)} cube metadata records")
            return cubes
            
        except Exception as e:
            self.logger.warning(f"catalogue/cubes failed ({e}). Trying catalogue/tables...")
            try:
                response = await self._make_request(ENDPOINT_CATALOGUE_TABLES, request_data)
                data = response.json()
                cubes: List[CubeInfo] = []
                for t in (data.get("List") or []):
                    cubes.append(CubeInfo(
                        code=t.get("Code", ""),
                        content=t.get("Content", ""),
                        latest_update=self._parse_datetime(t.get("Updated"))
                    ))
                cubes.sort(key=lambda x: x.latest_update or datetime.min, reverse=True)
                self.logger.info(f"Parsed {len(cubes)} records from catalogue/tables")
                return cubes
            except Exception as e2:
                self.logger.error(f"Failed via catalogue/tables ({e2}). Falling back to legacy list...")
                legacy_tables = await self.get_available_tables()
                return [CubeInfo(
                    code=table.name,
                    content=table.description,
                    latest_update=table.updated
                ) for table in legacy_tables]

    async def get_available_tables(self, filter_term: Optional[str] = None) -> List[TableInfo]:
        """Get list of available statistical tables."""
        request_data = {}
        if filter_term:
            request_data["term"] = filter_term
        
        # Prefer official catalogue/tables; fallback to deprecated find/table
        try:
            response = await self._make_request(ENDPOINT_CATALOGUE_TABLES, request_data)
        except Exception:
            response = await self._make_request("find/table", request_data)
        data = response.json()
        
        tables = []
        for table_data in (data.get("List") or []):
            table_info = TableInfo(
                name=table_data.get("Code", ""),
                description=table_data.get("Content", ""),
                updated=self._parse_datetime(table_data.get("Updated"))
            )
            tables.append(table_info)
        
        return tables
    
    async def fetch_data(self, **kwargs) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Fetch data using METADATA-FIRST approach with official cube endpoints.
        
        This method follows the official 2025 API pattern:
        1. Discovery via catalogue/cubes (metadata)
        2. Extraction via data/cubefile (raw cubes)
        
        Yields individual cube data records with complete metadata.
        """
        # Extract parameters
        cube_codes = kwargs.get("cube_codes", [])
        selection_pattern = kwargs.get("selection", "*")
        language = kwargs.get("language", "de")
        max_cubes = kwargs.get("max_cubes", None)
        
        try:
            # Phase 1: METADATA DISCOVERY (primary approach)
            if not cube_codes:
                self.logger.info("No specific cubes requested, discovering via metadata...")
                cubes = await self.get_available_cubes(
                    selection=selection_pattern,
                    language=language
                )
                
                if max_cubes:
                    cubes = cubes[:max_cubes]
                    self.logger.info(f"Limited to first {max_cubes} cubes")
                
                cube_codes = [cube.code for cube in cubes]
            
            # Phase 2: DATA EXTRACTION (cube-by-cube)
            for cube_code in cube_codes:
                try:
                    self.logger.info(f"Extracting cube: {cube_code}")
                    
                    # Extract raw cube data
                    file_path = await self.fetch_cube(cube_code, language)
                    
                    yield {
                        "cube_code": cube_code,
                        "file_path": str(file_path),
                        "status": "success",
                        "format": "raw_cube",
                        "language": language,
                        "timestamp": datetime.now().isoformat(),
                        "extraction_method": "official_cube_api"
                    }
                    
                except Exception as e:
                    self.logger.error(f"Failed to extract cube {cube_code}: {e}")
                    yield {
                        "cube_code": cube_code,
                        "status": "failed",
                        "error": str(e),
                        "timestamp": datetime.now().isoformat(),
                        "extraction_method": "official_cube_api"
                    }
        
        except Exception as e:
            self.logger.error(f"Metadata discovery failed: {e}")
            # Fallback to legacy table approach
            self.logger.warning("Falling back to legacy table extraction...")
            
            async for legacy_result in self._fetch_data_legacy(**kwargs):
                yield legacy_result

    async def _fetch_data_legacy(self, **kwargs) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Legacy fetch_data implementation for fallback compatibility.
        Uses the old data/table endpoint approach.
        """
        table_ids = kwargs.get("table_ids", [])
        area = kwargs.get("area", "de")
        start_year = kwargs.get("start_year")
        end_year = kwargs.get("end_year")
        fmt = kwargs.get("format", "json")
        
        for table_id in table_ids:
            try:
                file_path = await self.fetch_table(
                    table_id, area, start_year, end_year, fmt
                )
                
                yield {
                    "table_id": table_id,
                    "file_path": str(file_path),
                    "status": "success",
                    "format": fmt,
                    "timestamp": datetime.now().isoformat(),
                    "extraction_method": "legacy_table_api"
                }
                
            except Exception as e:
                self.logger.error(f"Failed to fetch table {table_id}: {e}")
                yield {
                    "table_id": table_id,
                    "status": "failed",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                    "extraction_method": "legacy_table_api"
                }
    
    async def get_incremental_data(
        self, 
        since: datetime, 
        **kwargs
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Fetch incremental data since timestamp using METADATA-FIRST approach.
        
        For GENESIS, this returns cubes updated since the given timestamp.
        Uses the official catalogue/cubes endpoint to check update times.
        """
        try:
            # Get all available cubes with metadata
            cubes = await self.get_available_cubes(
                selection=kwargs.get("selection", "*"),
                language=kwargs.get("language", "de")
            )
            
            # Filter cubes updated since the given timestamp
            updated_cubes = [
                cube for cube in cubes 
                if cube.latest_update and cube.latest_update > since
            ]
            
            self.logger.info(
                f"Found {len(updated_cubes)} cubes updated since {since.isoformat()}"
            )
            
            # Extract data for updated cubes
            async for data in self.fetch_data(
                cube_codes=[cube.code for cube in updated_cubes],
                **kwargs
            ):
                yield data
                
        except Exception as e:
            self.logger.error(f"Incremental data fetch failed: {e}")
            # Fallback to legacy approach
            async for legacy_result in self._get_incremental_data_legacy(since, **kwargs):
                yield legacy_result

    async def _get_incremental_data_legacy(
        self, 
        since: datetime, 
        **kwargs
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Legacy incremental data implementation for fallback."""
        tables = await self.get_available_tables()
        
        for table in tables:
            if table.updated and table.updated > since:
                async for data in self._fetch_data_legacy(table_ids=[table.name], **kwargs):
                    yield data


# CLI support
async def main():
    """CLI main function for testing and direct usage."""
    import argparse
    import os
    
    parser = argparse.ArgumentParser(description="DESTATIS GENESIS API connector")
    parser.add_argument("--table", required=True, help="Table ID (e.g. 12411-0001)")
    parser.add_argument("--token", help="API token (or set DESTATIS_TOKEN env var)")
    parser.add_argument("--area", default="all", help="Area code (default: all)")
    parser.add_argument("--format", default="json", choices=["json", "csv"], help="Output format")
    parser.add_argument("--start-year", type=int, help="Start year")
    parser.add_argument("--end-year", type=int, help="End year")
    
    args = parser.parse_args()
    
    # Get token from argument or environment
    token = args.token or os.getenv("DESTATIS_TOKEN")
    if not token:
        print("Error: No token provided. Use --token argument or set DESTATIS_TOKEN environment variable.")
        return 1
    
    # Create connector with token
    config = DestatisConfig(api_token=token)
    
    try:
        async with DestatisConnector(config) as connector:
            print(f"Fetching table {args.table}...")
            
            file_path = await connector.fetch_table(
                table_id=args.table,
                area=args.area,
                start_year=args.start_year,
                end_year=args.end_year,
                fmt=args.format
            )
            
            print(f"✅ Data saved to: {file_path}")
            print(f"📊 File size: {file_path.stat().st_size} bytes")
            return 0
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(asyncio.run(main()))
