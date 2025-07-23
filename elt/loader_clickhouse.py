"""
ClickHouse bulk data loader for ELT pipeline.
Provides efficient batch loading and streaming capabilities for the analytics warehouse.
"""

import logging
from typing import Dict, Any, List, Optional, Iterator, Union
from datetime import datetime
import json
import pandas as pd
import clickhouse_connect
from clickhouse_connect.driver import Client
from pydantic import BaseModel, Field
import os
from pathlib import Path

from .metadata import RawIngestion, IngestionStatus
from .utils.logging_config import get_logger


class ClickHouseConfig(BaseModel):
    """Configuration for ClickHouse connection."""
    host: str = Field(default_factory=lambda: os.getenv("CLICKHOUSE_HOST", "localhost"))
    port: int = Field(default_factory=lambda: int(os.getenv("CLICKHOUSE_PORT", "8124")))
    username: str = Field(default_factory=lambda: os.getenv("CLICKHOUSE_USER", "default"))
    password: str = Field(default_factory=lambda: os.getenv("CLICKHOUSE_PASSWORD", ""))
    database: str = Field(default_factory=lambda: os.getenv("CLICKHOUSE_DATABASE", "analytics"))
    connect_timeout: int = 30
    send_receive_timeout: int = 300
    compress: bool = True


class ClickHouseLoader:
    """
    Bulk data loader for ClickHouse analytics warehouse.
    
    Provides methods for:
    - Bulk loading from various data sources
    - Streaming data insertion
    - Upsert operations with ReplacingMergeTree
    - Schema management and table creation
    """
    
    def __init__(self, config: Optional[ClickHouseConfig] = None):
        self.config = config or ClickHouseConfig()
        self.client: Optional[Client] = None
        self.logger = get_logger(self.__class__.__name__)
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
    
    def connect(self) -> None:
        """Establish connection to ClickHouse."""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                username=self.config.username,
                password=self.config.password,
                database=self.config.database,
                connect_timeout=self.config.connect_timeout,
                send_receive_timeout=self.config.send_receive_timeout,
                compress=self.config.compress
            )
            
            # Test connection
            self.client.ping()
            self.logger.info(f"Connected to ClickHouse at {self.config.host}:{self.config.port}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close connection to ClickHouse."""
        if self.client:
            self.client.close()
            self.client = None
            self.logger.info("Disconnected from ClickHouse")
    
    def create_database(self, database_name: str) -> None:
        """Create database if it doesn't exist."""
        try:
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            self.logger.info(f"Created database: {database_name}")
        except Exception as e:
            self.logger.error(f"Failed to create database {database_name}: {e}")
            raise
    
    def create_table(self, table_name: str, schema: str) -> None:
        """Create table with given schema."""
        try:
            self.client.command(f"CREATE TABLE IF NOT EXISTS {table_name} {schema}")
            self.logger.info(f"Created table: {table_name}")
        except Exception as e:
            self.logger.error(f"Failed to create table {table_name}: {e}")
            raise
    
    def insert_dataframe(
        self,
        table_name: str,
        df: pd.DataFrame,
        batch_size: int = 10000
    ) -> int:
        """
        Insert DataFrame into ClickHouse table in batches.
        
        Args:
            table_name: Target table name
            df: DataFrame to insert
            batch_size: Number of rows per batch
            
        Returns:
            Number of rows inserted
        """
        if self.client is None:
            raise RuntimeError("Not connected to ClickHouse")
        
        try:
            total_rows = len(df)
            inserted_rows = 0
            
            for i in range(0, total_rows, batch_size):
                batch = df.iloc[i:i + batch_size]
                
                # Convert DataFrame to list of dictionaries
                data = batch.to_dict('records')
                
                # Insert batch
                self.client.insert(table_name, data)
                inserted_rows += len(data)
                
                self.logger.debug(f"Inserted batch {i//batch_size + 1}: {len(data)} rows")
            
            self.logger.info(f"Inserted {inserted_rows} rows into {table_name}")
            return inserted_rows
            
        except Exception as e:
            self.logger.error(f"Failed to insert DataFrame into {table_name}: {e}")
            raise
    
    def insert_json_data(
        self,
        table_name: str,
        json_data: List[Dict[str, Any]],
        batch_size: int = 10000
    ) -> int:
        """
        Insert JSON data into ClickHouse table.
        
        Args:
            table_name: Target table name
            json_data: List of dictionaries to insert
            batch_size: Number of rows per batch
            
        Returns:
            Number of rows inserted
        """
        if self.client is None:
            raise RuntimeError("Not connected to ClickHouse")
        
        try:
            total_rows = len(json_data)
            inserted_rows = 0
            
            for i in range(0, total_rows, batch_size):
                batch = json_data[i:i + batch_size]
                
                # Insert batch
                self.client.insert(table_name, batch)
                inserted_rows += len(batch)
                
                self.logger.debug(f"Inserted batch {i//batch_size + 1}: {len(batch)} rows")
            
            self.logger.info(f"Inserted {inserted_rows} rows into {table_name}")
            return inserted_rows
            
        except Exception as e:
            self.logger.error(f"Failed to insert JSON data into {table_name}: {e}")
            raise
    
    def stream_insert(
        self,
        table_name: str,
        data_iterator: Iterator[Dict[str, Any]],
        batch_size: int = 1000
    ) -> int:
        """
        Stream data insertion for large datasets.
        
        Args:
            table_name: Target table name
            data_iterator: Iterator yielding data dictionaries
            batch_size: Number of rows per batch
            
        Returns:
            Number of rows inserted
        """
        if self.client is None:
            raise RuntimeError("Not connected to ClickHouse")
        
        try:
            batch = []
            total_inserted = 0
            
            for item in data_iterator:
                batch.append(item)
                
                if len(batch) >= batch_size:
                    # Insert batch
                    self.client.insert(table_name, batch)
                    total_inserted += len(batch)
                    self.logger.debug(f"Streamed batch: {len(batch)} rows")
                    batch = []
            
            # Insert remaining items
            if batch:
                self.client.insert(table_name, batch)
                total_inserted += len(batch)
                self.logger.debug(f"Streamed final batch: {len(batch)} rows")
            
            self.logger.info(f"Streamed {total_inserted} rows into {table_name}")
            return total_inserted
            
        except Exception as e:
            self.logger.error(f"Failed to stream data into {table_name}: {e}")
            raise
    
    def upsert_data(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        primary_key: str
    ) -> int:
        """
        Upsert data using ReplacingMergeTree engine.
        
        Args:
            table_name: Target table name (must use ReplacingMergeTree)
            data: List of dictionaries to upsert
            primary_key: Primary key column name
            
        Returns:
            Number of rows processed
        """
        if self.client is None:
            raise RuntimeError("Not connected to ClickHouse")
        
        try:
            # Insert data (ReplacingMergeTree will handle duplicates)
            inserted_rows = self.insert_json_data(table_name, data)
            
            # Optimize table to trigger merge
            self.client.command(f"OPTIMIZE TABLE {table_name} FINAL")
            
            self.logger.info(f"Upserted {inserted_rows} rows into {table_name}")
            return inserted_rows
            
        except Exception as e:
            self.logger.error(f"Failed to upsert data into {table_name}: {e}")
            raise
    
    def load_from_file(
        self,
        table_name: str,
        file_path: Union[str, Path],
        file_format: str = "auto"
    ) -> int:
        """
        Load data from file into ClickHouse table.
        
        Args:
            table_name: Target table name
            file_path: Path to data file
            file_format: File format (auto, csv, json, parquet)
            
        Returns:
            Number of rows loaded
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Auto-detect format
        if file_format == "auto":
            suffix = file_path.suffix.lower()
            if suffix == ".csv":
                file_format = "csv"
            elif suffix == ".json":
                file_format = "json"
            elif suffix == ".parquet":
                file_format = "parquet"
            else:
                raise ValueError(f"Unsupported file format: {suffix}")
        
        try:
            if file_format == "csv":
                df = pd.read_csv(file_path)
                return self.insert_dataframe(table_name, df)
            
            elif file_format == "json":
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        return self.insert_json_data(table_name, data)
                    else:
                        return self.insert_json_data(table_name, [data])
            
            elif file_format == "parquet":
                df = pd.read_parquet(file_path)
                return self.insert_dataframe(table_name, df)
            
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
                
        except Exception as e:
            self.logger.error(f"Failed to load file {file_path}: {e}")
            raise
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute SQL query and return results."""
        if self.client is None:
            raise RuntimeError("Not connected to ClickHouse")
        
        try:
            result = self.client.query(query)
            return result.result_rows
        except Exception as e:
            self.logger.error(f"Failed to execute query: {e}")
            raise
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get table information and statistics."""
        if self.client is None:
            raise RuntimeError("Not connected to ClickHouse")
        
        try:
            # Get table schema
            schema_query = f"DESCRIBE TABLE {table_name}"
            schema = self.client.query(schema_query).result_rows
            
            # Get row count
            count_query = f"SELECT COUNT(*) as count FROM {table_name}"
            count = self.client.query(count_query).result_rows[0][0]
            
            # Get table size
            size_query = f"""
                SELECT 
                    formatReadableSize(sum(bytes)) as size,
                    sum(rows) as rows
                FROM system.parts 
                WHERE table = '{table_name.split('.')[-1]}'
            """
            size_info = self.client.query(size_query).result_rows[0]
            
            return {
                'name': table_name,
                'schema': schema,
                'row_count': count,
                'size': size_info[0] if size_info else 'Unknown',
                'rows': size_info[1] if size_info else count
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get table info for {table_name}: {e}")
            raise
    
    def log_ingestion(self, ingestion: RawIngestion) -> None:
        """Log ingestion metadata to tracking table."""
        try:
            # Create ingestion tracking table if it doesn't exist
            self.create_table(
                "raw.ingestions",
                """
                (
                    id UUID DEFAULT generateUUIDv4(),
                    source String,
                    dataset String,
                    ingestion_time DateTime64(3),
                    records_count UInt64,
                    file_path String,
                    status String,
                    error_message String,
                    metadata String
                )
                ENGINE = MergeTree()
                ORDER BY (source, dataset, ingestion_time)
                """
            )
            
            # Insert ingestion record
            ingestion_data = {
                'source': ingestion.source,
                'dataset': ingestion.dataset,
                'ingestion_time': ingestion.timestamp,
                'records_count': ingestion.records_count,
                'file_path': ingestion.file_path or '',
                'status': ingestion.status.value,
                'error_message': ingestion.error_message or '',
                'metadata': json.dumps(ingestion.metadata or {})
            }
            
            self.client.insert("raw.ingestions", [ingestion_data])
            self.logger.info(f"Logged ingestion: {ingestion.source}.{ingestion.dataset}")
            
        except Exception as e:
            self.logger.error(f"Failed to log ingestion: {e}")
            # Don't raise - logging failure shouldn't stop the pipeline
        
    def load_dicts(self, data: List[Dict[str, Any]], table: str, batch_size: int = 10000) -> int:
        """
        Load list of dictionaries into ClickHouse table with batching.
        
        Args:
            data: List of dictionaries to load
            table: Target table name
            batch_size: Number of rows per batch (default 10000)
            
        Returns:
            Number of rows loaded
        """
        if self.client is None:
            raise RuntimeError("Not connected to ClickHouse")
        
        if not data:
            self.logger.warning(f"No data provided to load into {table}")
            return 0
        
        try:
            # Get table structure to map columns
            describe_result = self.client.query(f'DESCRIBE TABLE {table}')
            # Filter out materialized columns
            column_names = [row[0] for row in describe_result.result_rows 
                           if not row[2] or 'MATERIALIZED' not in row[2]]
            
            total_rows = len(data)
            loaded_rows = 0
            
            # Process in batches
            for i in range(0, total_rows, batch_size):
                batch = data[i:i + batch_size]
                
                # Convert dictionaries to list of lists in column order
                batch_rows = []
                for row_dict in batch:
                    row_list = [row_dict.get(col, None) for col in column_names]
                    batch_rows.append(row_list)
                
                # Insert batch
                self.client.insert(table, batch_rows, column_names=column_names)
                loaded_rows += len(batch)
                
                self.logger.debug(f"Loaded batch {i//batch_size + 1}: {len(batch)} rows into {table}")
            
            self.logger.info(f"Successfully loaded {loaded_rows} rows into {table}")
            return loaded_rows
            
        except Exception as e:
            self.logger.error(f"Failed to load data into {table}: {e}")
            raise
    
    def create_raw_dawum_table(self) -> None:
        """Create the raw.dawum_polls table with proper schema."""
        ddl = """
        CREATE TABLE IF NOT EXISTS raw.dawum_polls (
            poll_id String,
            institute_id Nullable(String),
            institute_name Nullable(String),
            tasker_id Nullable(String),
            tasker_name Nullable(String),
            parliament_id Nullable(String),
            parliament_name Nullable(String),
            method_id Nullable(String),
            method_name Nullable(String),
            survey_period_start Nullable(DateTime),
            survey_period_end Nullable(DateTime),
            publication_date Nullable(DateTime),
            sample_size Nullable(Int32),
            results String,
            source_url Nullable(String),
            source_loaded_at DateTime,
            created_at DateTime,
            updated_at DateTime,
            event_date Date MATERIALIZED toDate(ifNull(publication_date, now()))
        ) ENGINE = ReplacingMergeTree(updated_at)
        PARTITION BY toYYYYMM(event_date)
        ORDER BY poll_id
        """
        
        try:
            # Create raw database if not exists
            self.client.command("CREATE DATABASE IF NOT EXISTS raw")
            
            # Create table
            self.client.command(ddl)
            self.logger.info("Created raw.dawum_polls table")
            
        except Exception as e:
            self.logger.error(f"Failed to create raw.dawum_polls table: {e}")
            raise
    
    def upsert_dawum_polls(self, data: List[Dict[str, Any]], batch_size: int = 100) -> int:
        """
        Upsert DAWUM polls data using ReplacingMergeTree strategy.
        
        Args:
            data: List of poll dictionaries
            
        Returns:
            Number of rows processed
        """
        if not data:
            return 0
        
        try:
            # Ensure table exists
            self.create_raw_dawum_table()
            
            # Extract poll_ids for deletion
            poll_ids = [poll['poll_id'] for poll in data if poll.get('poll_id')]
            
            if poll_ids:
                # Delete existing records for these poll_ids
                poll_ids_str = "', '".join(poll_ids)
                delete_query = f"ALTER TABLE raw.dawum_polls DELETE WHERE poll_id IN ('{poll_ids_str}')"
                self.client.command(delete_query)
                self.logger.info(f"Deleted existing records for {len(poll_ids)} poll_ids")
            
            # Insert new data
            rows_loaded = self.load_dicts(data, "raw.dawum_polls", batch_size=batch_size)
            
            # Optimize table to trigger merge
            self.client.command("OPTIMIZE TABLE raw.dawum_polls FINAL")
            
            return rows_loaded
            
        except Exception as e:
            self.logger.error(f"Failed to upsert DAWUM polls: {e}")
            raise


# Example usage and utility functions
if __name__ == "__main__":
    # Example usage
    with ClickHouseLoader() as loader:
        # Create test data
        test_data = [
            {'id': 1, 'name': 'Test 1', 'value': 100},
            {'id': 2, 'name': 'Test 2', 'value': 200},
        ]
        
        # Insert test data
        loader.insert_json_data("test_table", test_data)
        
        # Get table info
        info = loader.get_table_info("test_table")
        print(f"Table info: {info}")
