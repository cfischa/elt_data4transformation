"""
main.py
-------
Inputs:   None (runs DawumConnector as a test fetch).
Outputs:  Saves all poll data and mappings to the filesystem.
Functionality:
    - Entry point for running data acquisition from the DAWUM API.
    - Sets up logging to both console and file.
    - Fetches and stores complete API data including:
        - Polls (Surveys)
        - Parties
        - Parliaments
        - Institutes
        - Taskers
        - Methods
        - Database information
    - Tracks known poll IDs to identify new polls.
    - Provides detailed logging of the acquisition process.
"""

import os
import logging
from data_acquisition.connectors.dawum_connector import DawumConnector
from data_acquisition.utils.logging_config import setup_logging
from data_acquisition.utils.persistence import (
    load_known_ids, 
    save_known_ids, 
    save_polls_dict,
    save_all_api_mappings
)

# Configuration constants
SOURCE = "dawum"
BASE_DIR = "data"


def setup_environment():
    """Set up logging and directory structure."""
    setup_logging(log_to_file=True)
    logging.info(f"Starting data acquisition for {SOURCE}")
    
    # Create directory structure
    os.makedirs(os.path.join(BASE_DIR, SOURCE), exist_ok=True)
    
    return load_known_ids(source=SOURCE, base_dir=BASE_DIR)


def fetch_api_data():
    """Fetch data from the DAWUM API."""
    connector = DawumConnector()
    logging.info("Fetching data from API...")
    data = connector.fetch_polls()
    logging.info(f"API response data type: {type(data)}")
    
    if not isinstance(data, dict):
        logging.error(f"Unexpected response type from DAWUM API: {type(data)}")
        return None
    
    logging.info(f"API response keys: {list(data.keys())}")
    return data


def save_mappings(api_data):
    """Save all API mappings to disk."""
    saved_mappings = save_all_api_mappings(api_data, source=SOURCE, base_dir=BASE_DIR)
    logging.info(f"Saved {len(saved_mappings)} API mappings: {list(saved_mappings.keys())}")
    return saved_mappings


def process_and_save_polls(api_data, known_ids):
    """Process and save polls data, updating known IDs."""
    if 'Surveys' not in api_data:
        logging.error(f"No 'Surveys' key found in API response. Available keys: {list(api_data.keys())}")
        return
    
    poll_dicts = api_data['Surveys']
    logging.info(f"Fetched {len(poll_dicts)} total polls")
    
    # Identify new polls
    new_polls = {poll_id: poll for poll_id, poll in poll_dicts.items() if poll_id not in known_ids}
    logging.info(f"Found {len(new_polls)} new polls")
    
    # Update known IDs
    all_ids = known_ids.union(set(poll_dicts.keys()))
    save_known_ids(all_ids, source=SOURCE, base_dir=BASE_DIR)
    logging.info(f"Updated known IDs list with {len(all_ids)} total IDs")
    
    # Save polls data
    if poll_dicts:
        logging.info(f"Saving polls dictionary with {len(poll_dicts)} entries")
        saved_path = save_polls_dict(poll_dicts, source=SOURCE, base_dir=BASE_DIR)
        logging.info(f"Saved all polls to {saved_path}")
    else:
        logging.info("No polls to save")


def main():
    """Main function to orchestrate the data acquisition process."""
    # Setup environment and get known IDs
    known_ids = setup_environment()
    logging.info(f"Loaded {len(known_ids)} known poll IDs")
    
    # Fetch data from API
    api_data = fetch_api_data()
    if not api_data:
        return
    
    # Save all mappings
    save_mappings(api_data)
    
    # Process and save polls data
    process_and_save_polls(api_data, known_ids)
    
    logging.info("Data acquisition completed successfully")


if __name__ == "__main__":
    main()


