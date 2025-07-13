"""
logging_config
--------------
Inputs:   None (setup function used at application start).
Outputs:  Sets up logging for the entire project.
Functionality:
    - Configures Python logging for use throughout the project.
    - Logs to both console and file for monitoring
"""

import logging
import os
from datetime import datetime

def setup_logging(log_to_file=True):
    """
    Set up logging for the application.
    
    Args:
        log_to_file: If True, log to file in addition to console
    """
    # Create logs directory if it doesn't exist
    if log_to_file:
        log_dir = os.path.join("data", "logs")
        os.makedirs(log_dir, exist_ok=True)
        
        # Create log filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"dawum_connector_{timestamp}.log")
        
        # Configure file handler with explicit UTF-8 encoding
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        
        # Configure handlers
        handlers = [
            logging.StreamHandler(),  # Console handler
            file_handler              # File handler with UTF-8 encoding
        ]
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            handlers=handlers
        )
        
        logging.info(f"Logging to file: {log_file}")
    else:
        # Just log to console
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
