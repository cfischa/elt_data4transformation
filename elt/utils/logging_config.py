"""
Structured logging configuration for the ELT pipeline.
Provides consistent logging across all components with different output formats.
"""

import logging
import logging.config
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import json
import os
from pythonjsonlogger import jsonlogger


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """Custom JSON formatter with additional metadata."""
    
    def add_fields(self, log_record: Dict[str, Any], record: logging.LogRecord, message_dict: Dict[str, Any]) -> None:
        super().add_fields(log_record, record, message_dict)
        
        # Add timestamp
        log_record['timestamp'] = datetime.utcnow().isoformat()
        
        # Add application context
        log_record['app'] = 'bnb-data4transformation'
        
        # Add source location
        log_record['source'] = f"{record.name}:{record.lineno}"
        
        # Add environment
        log_record['environment'] = os.getenv('ENVIRONMENT', 'development')


class ContextFilter(logging.Filter):
    """Filter to add context information to log records."""
    
    def __init__(self, context: Optional[Dict[str, Any]] = None):
        super().__init__()
        self.context = context or {}
    
    def filter(self, record: logging.LogRecord) -> bool:
        # Add context to record
        for key, value in self.context.items():
            setattr(record, key, value)
        return True


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    enable_json: bool = False,
    context: Optional[Dict[str, Any]] = None
) -> None:
    """
    Set up logging configuration for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (optional)
        enable_json: Enable JSON formatting for structured logging
        context: Additional context to include in logs
    """
    # Create logs directory if it doesn't exist
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    handlers = {}
    formatters = {}
    
    # Console handler
    if enable_json:
        formatters['json'] = {
            'class': 'elt.utils.logging_config.CustomJsonFormatter',
            'format': '%(asctime)s %(name)s %(levelname)s %(message)s'
        }
        handlers['console'] = {
            'class': 'logging.StreamHandler',
            'level': log_level,
            'formatter': 'json',
            'stream': 'ext://sys.stdout'
        }
    else:
        formatters['detailed'] = {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s [%(filename)s:%(lineno)d]'
        }
        handlers['console'] = {
            'class': 'logging.StreamHandler',
            'level': log_level,
            'formatter': 'detailed',
            'stream': 'ext://sys.stdout'
        }
    
    # File handler
    if log_file:
        if enable_json:
            handlers['file'] = {
                'class': 'logging.handlers.RotatingFileHandler',
                'level': log_level,
                'formatter': 'json',
                'filename': log_file,
                'maxBytes': 10485760,  # 10MB
                'backupCount': 5,
                'encoding': 'utf8'
            }
        else:
            handlers['file'] = {
                'class': 'logging.handlers.RotatingFileHandler',
                'level': log_level,
                'formatter': 'detailed',
                'filename': log_file,
                'maxBytes': 10485760,  # 10MB
                'backupCount': 5,
                'encoding': 'utf8'
            }
    
    # Root logger configuration
    root_config = {
        'level': log_level,
        'handlers': list(handlers.keys())
    }
    
    # Full configuration
    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': formatters,
        'handlers': handlers,
        'root': root_config,
        'loggers': {
            'elt': {
                'level': log_level,
                'propagate': True
            },
            'connectors': {
                'level': log_level,
                'propagate': True
            },
            'scraping': {
                'level': log_level,
                'propagate': True
            },
            # Reduce noise from third-party libraries
            'urllib3': {
                'level': 'WARNING',
                'propagate': True
            },
            'requests': {
                'level': 'WARNING',
                'propagate': True
            },
            'asyncio': {
                'level': 'WARNING',
                'propagate': True
            }
        }
    }
    
    # Apply configuration
    logging.config.dictConfig(config)
    
    # Add context filter if provided
    if context:
        context_filter = ContextFilter(context)
        root_logger = logging.getLogger()
        root_logger.addFilter(context_filter)


def get_logger(name: str, context: Optional[Dict[str, Any]] = None) -> logging.Logger:
    """
    Get a logger with the specified name and optional context.
    
    Args:
        name: Logger name
        context: Additional context to include in logs
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Add context filter if provided
    if context:
        context_filter = ContextFilter(context)
        logger.addFilter(context_filter)
    
    return logger


def configure_pipeline_logging(
    pipeline_name: str,
    run_id: str,
    log_level: str = "INFO",
    enable_json: bool = True
) -> logging.Logger:
    """
    Configure logging for a specific pipeline run.
    
    Args:
        pipeline_name: Name of the pipeline
        run_id: Unique identifier for the pipeline run
        log_level: Logging level
        enable_json: Enable JSON formatting
        
    Returns:
        Configured logger for the pipeline
    """
    # Create log file path
    log_dir = Path(os.getenv('LOG_FILE_PATH', './data/logs'))
    log_file = log_dir / f"{pipeline_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Pipeline context
    context = {
        'pipeline_name': pipeline_name,
        'run_id': run_id,
        'environment': os.getenv('ENVIRONMENT', 'development')
    }
    
    # Setup logging
    setup_logging(
        log_level=log_level,
        log_file=str(log_file),
        enable_json=enable_json,
        context=context
    )
    
    # Return logger
    return get_logger(pipeline_name, context)


class LoggerMixin:
    """Mixin class to add logging capabilities to other classes."""
    
    @property
    def logger(self) -> logging.Logger:
        """Get logger for this class."""
        return get_logger(self.__class__.__name__)


class TimedLogger:
    """Context manager for timing operations with logging."""
    
    def __init__(self, logger: logging.Logger, operation: str, level: int = logging.INFO):
        self.logger = logger
        self.operation = operation
        self.level = level
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.log(self.level, f"Starting {self.operation}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = datetime.now() - self.start_time
        
        if exc_type is None:
            self.logger.log(self.level, f"Completed {self.operation} in {duration.total_seconds():.2f} seconds")
        else:
            self.logger.error(f"Failed {self.operation} after {duration.total_seconds():.2f} seconds: {exc_val}")


# Default configuration
def configure_default_logging():
    """Configure default logging for the application."""
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    log_file_path = os.getenv('LOG_FILE_PATH', './data/logs')
    enable_json = os.getenv('ENABLE_JSON_LOGGING', 'false').lower() == 'true'
    
    # Create log file name
    log_file = Path(log_file_path) / f"bnb_data4transformation_{datetime.now().strftime('%Y%m%d')}.log"
    
    setup_logging(
        log_level=log_level,
        log_file=str(log_file),
        enable_json=enable_json
    )


# Configure logging on module import
if not logging.getLogger().handlers:
    configure_default_logging()


# Example usage
if __name__ == "__main__":
    # Test logging configuration
    logger = get_logger("test_logger")
    
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    
    # Test timed logger
    with TimedLogger(logger, "test operation"):
        import time
        time.sleep(1)
    
    # Test pipeline logging
    pipeline_logger = configure_pipeline_logging("test_pipeline", "run_123")
    pipeline_logger.info("Pipeline started")
    pipeline_logger.info("Pipeline completed")
