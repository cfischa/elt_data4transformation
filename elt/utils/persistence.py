"""
Persistence utilities for file I/O operations.
Provides helpers for saving and loading data in various formats.
"""

import json
import pickle
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import os
import shutil
from contextlib import contextmanager
import tempfile

from .logging_config import get_logger


class PersistenceManager:
    """Manager for file persistence operations."""
    
    def __init__(self, base_path: Optional[Union[str, Path]] = None):
        self.base_path = Path(base_path) if base_path else Path("./data")
        self.logger = get_logger(self.__class__.__name__)
        
        # Create base directory if it doesn't exist
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def save_json(
        self,
        data: Union[Dict[str, Any], List[Any]],
        filename: str,
        subfolder: Optional[str] = None,
        indent: int = 2
    ) -> Path:
        """
        Save data as JSON file.
        
        Args:
            data: Data to save
            filename: Filename (without extension)
            subfolder: Optional subfolder
            indent: JSON indentation
            
        Returns:
            Path to saved file
        """
        file_path = self._get_file_path(filename, ".json", subfolder)
        
        try:
            # Ensure directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=indent, ensure_ascii=False, default=str)
            
            self.logger.info(f"Saved JSON file: {file_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Failed to save JSON file {file_path}: {e}")
            raise
    
    def load_json(
        self,
        filename: str,
        subfolder: Optional[str] = None
    ) -> Union[Dict[str, Any], List[Any]]:
        """
        Load data from JSON file.
        
        Args:
            filename: Filename (without extension)
            subfolder: Optional subfolder
            
        Returns:
            Loaded data
        """
        file_path = self._get_file_path(filename, ".json", subfolder)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.logger.info(f"Loaded JSON file: {file_path}")
            return data
            
        except FileNotFoundError:
            self.logger.error(f"JSON file not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in file {file_path}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to load JSON file {file_path}: {e}")
            raise
    
    def save_parquet(
        self,
        data: Union[pd.DataFrame, Dict[str, Any], List[Dict[str, Any]]],
        filename: str,
        subfolder: Optional[str] = None,
        compression: str = 'snappy'
    ) -> Path:
        """
        Save data as Parquet file.
        
        Args:
            data: Data to save (DataFrame or dict/list)
            filename: Filename (without extension)
            subfolder: Optional subfolder
            compression: Compression algorithm
            
        Returns:
            Path to saved file
        """
        file_path = self._get_file_path(filename, ".parquet", subfolder)
        
        try:
            # Ensure directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert to DataFrame if needed
            if isinstance(data, pd.DataFrame):
                df = data
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                raise ValueError(f"Unsupported data type: {type(data)}")
            
            # Save as Parquet
            df.to_parquet(file_path, compression=compression, index=False)
            
            self.logger.info(f"Saved Parquet file: {file_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Failed to save Parquet file {file_path}: {e}")
            raise
    
    def load_parquet(
        self,
        filename: str,
        subfolder: Optional[str] = None,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Load data from Parquet file.
        
        Args:
            filename: Filename (without extension)
            subfolder: Optional subfolder
            columns: Optional list of columns to load
            
        Returns:
            Loaded DataFrame
        """
        file_path = self._get_file_path(filename, ".parquet", subfolder)
        
        try:
            df = pd.read_parquet(file_path, columns=columns)
            self.logger.info(f"Loaded Parquet file: {file_path}")
            return df
            
        except FileNotFoundError:
            self.logger.error(f"Parquet file not found: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to load Parquet file {file_path}: {e}")
            raise
    
    def save_csv(
        self,
        data: Union[pd.DataFrame, List[Dict[str, Any]]],
        filename: str,
        subfolder: Optional[str] = None,
        **kwargs
    ) -> Path:
        """
        Save data as CSV file.
        
        Args:
            data: Data to save
            filename: Filename (without extension)
            subfolder: Optional subfolder
            **kwargs: Additional arguments for pandas.to_csv
            
        Returns:
            Path to saved file
        """
        file_path = self._get_file_path(filename, ".csv", subfolder)
        
        try:
            # Ensure directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert to DataFrame if needed
            if isinstance(data, pd.DataFrame):
                df = data
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                raise ValueError(f"Unsupported data type: {type(data)}")
            
            # Default CSV options
            csv_options = {
                'index': False,
                'encoding': 'utf-8'
            }
            csv_options.update(kwargs)
            
            df.to_csv(file_path, **csv_options)
            
            self.logger.info(f"Saved CSV file: {file_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Failed to save CSV file {file_path}: {e}")
            raise
    
    def load_csv(
        self,
        filename: str,
        subfolder: Optional[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Load data from CSV file.
        
        Args:
            filename: Filename (without extension)
            subfolder: Optional subfolder
            **kwargs: Additional arguments for pandas.read_csv
            
        Returns:
            Loaded DataFrame
        """
        file_path = self._get_file_path(filename, ".csv", subfolder)
        
        try:
            # Default CSV options
            csv_options = {
                'encoding': 'utf-8'
            }
            csv_options.update(kwargs)
            
            df = pd.read_csv(file_path, **csv_options)
            self.logger.info(f"Loaded CSV file: {file_path}")
            return df
            
        except FileNotFoundError:
            self.logger.error(f"CSV file not found: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to load CSV file {file_path}: {e}")
            raise
    
    def save_pickle(
        self,
        data: Any,
        filename: str,
        subfolder: Optional[str] = None
    ) -> Path:
        """
        Save data as pickle file.
        
        Args:
            data: Data to save
            filename: Filename (without extension)
            subfolder: Optional subfolder
            
        Returns:
            Path to saved file
        """
        file_path = self._get_file_path(filename, ".pkl", subfolder)
        
        try:
            # Ensure directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'wb') as f:
                pickle.dump(data, f)
            
            self.logger.info(f"Saved pickle file: {file_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Failed to save pickle file {file_path}: {e}")
            raise
    
    def load_pickle(
        self,
        filename: str,
        subfolder: Optional[str] = None
    ) -> Any:
        """
        Load data from pickle file.
        
        Args:
            filename: Filename (without extension)
            subfolder: Optional subfolder
            
        Returns:
            Loaded data
        """
        file_path = self._get_file_path(filename, ".pkl", subfolder)
        
        try:
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
            
            self.logger.info(f"Loaded pickle file: {file_path}")
            return data
            
        except FileNotFoundError:
            self.logger.error(f"Pickle file not found: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to load pickle file {file_path}: {e}")
            raise
    
    def file_exists(
        self,
        filename: str,
        extension: str = "",
        subfolder: Optional[str] = None
    ) -> bool:
        """Check if file exists."""
        file_path = self._get_file_path(filename, extension, subfolder)
        return file_path.exists()
    
    def get_file_info(
        self,
        filename: str,
        extension: str = "",
        subfolder: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get file information."""
        file_path = self._get_file_path(filename, extension, subfolder)
        
        if not file_path.exists():
            return {'exists': False}
        
        stat = file_path.stat()
        return {
            'exists': True,
            'size': stat.st_size,
            'modified': datetime.fromtimestamp(stat.st_mtime),
            'created': datetime.fromtimestamp(stat.st_ctime),
            'path': str(file_path)
        }
    
    def delete_file(
        self,
        filename: str,
        extension: str = "",
        subfolder: Optional[str] = None
    ) -> bool:
        """Delete file."""
        file_path = self._get_file_path(filename, extension, subfolder)
        
        try:
            if file_path.exists():
                file_path.unlink()
                self.logger.info(f"Deleted file: {file_path}")
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to delete file {file_path}: {e}")
            raise
    
    def list_files(
        self,
        subfolder: Optional[str] = None,
        pattern: str = "*",
        extension: Optional[str] = None
    ) -> List[Path]:
        """List files in directory."""
        if subfolder:
            search_path = self.base_path / subfolder
        else:
            search_path = self.base_path
        
        if not search_path.exists():
            return []
        
        # Build glob pattern
        if extension:
            pattern = f"*.{extension.lstrip('.')}"
        
        files = list(search_path.glob(pattern))
        return [f for f in files if f.is_file()]
    
    def create_backup(
        self,
        filename: str,
        extension: str = "",
        subfolder: Optional[str] = None
    ) -> Optional[Path]:
        """Create backup of file."""
        file_path = self._get_file_path(filename, extension, subfolder)
        
        if not file_path.exists():
            return None
        
        # Create backup filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
        backup_path = file_path.parent / backup_name
        
        try:
            shutil.copy2(file_path, backup_path)
            self.logger.info(f"Created backup: {backup_path}")
            return backup_path
            
        except Exception as e:
            self.logger.error(f"Failed to create backup of {file_path}: {e}")
            raise
    
    def _get_file_path(
        self,
        filename: str,
        extension: str = "",
        subfolder: Optional[str] = None
    ) -> Path:
        """Get full file path."""
        if subfolder:
            path = self.base_path / subfolder
        else:
            path = self.base_path
        
        # Add extension if not present
        if extension and not filename.endswith(extension):
            filename += extension
        
        return path / filename
    
    @contextmanager
    def temp_file(self, suffix: str = ".tmp"):
        """Context manager for temporary file."""
        temp_path = None
        try:
            temp_fd, temp_path = tempfile.mkstemp(suffix=suffix, dir=self.base_path)
            os.close(temp_fd)  # Close file descriptor
            yield Path(temp_path)
        finally:
            if temp_path and Path(temp_path).exists():
                Path(temp_path).unlink()


# Convenience functions
def save_json(data: Any, filename: str, base_path: Optional[str] = None) -> Path:
    """Save data as JSON file."""
    manager = PersistenceManager(base_path)
    return manager.save_json(data, filename)


def load_json(filename: str, base_path: Optional[str] = None) -> Any:
    """Load data from JSON file."""
    manager = PersistenceManager(base_path)
    return manager.load_json(filename)


def save_parquet(data: Any, filename: str, base_path: Optional[str] = None) -> Path:
    """Save data as Parquet file."""
    manager = PersistenceManager(base_path)
    return manager.save_parquet(data, filename)


def load_parquet(filename: str, base_path: Optional[str] = None) -> pd.DataFrame:
    """Load data from Parquet file."""
    manager = PersistenceManager(base_path)
    return manager.load_parquet(filename)


# Example usage
if __name__ == "__main__":
    # Test persistence manager
    pm = PersistenceManager("./test_data")
    
    # Test JSON
    test_data = {"message": "Hello, World!", "timestamp": datetime.now()}
    json_path = pm.save_json(test_data, "test", "json")
    loaded_data = pm.load_json("test", "json")
    print(f"JSON test: {loaded_data}")
    
    # Test Parquet
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [10.5, 20.3, 30.1]
    })
    parquet_path = pm.save_parquet(df, "test", "parquet")
    loaded_df = pm.load_parquet("test", "parquet")
    print(f"Parquet test: {loaded_df.shape}")
    
    # Clean up
    pm.delete_file("test", ".json", "json")
    pm.delete_file("test", ".parquet", "parquet")
