#Todo: make sure that an id is known when in the DB

"""
Persistence utilities for tracking known poll IDs and storing poll data.

- Save/load IDs from local disk.
- Support for organizing data by source (e.g., dawum)
- Store poll content in structured folders
"""
import json
import os
from typing import Set, Dict, Any, List

def ensure_directory_exists(path: str) -> None:
    """Ensure the directory exists, creating it if necessary."""
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

def load_known_ids(source: str = "dawum", base_dir: str = "data") -> Set[str]:
    """
    Load known IDs from a JSON file.
    
    Args:
        source: Data source identifier (e.g., 'dawum')
        base_dir: Base directory for data storage
    
    Returns:
        Set of known IDs
    """
    path = os.path.join(base_dir, source, "known_ids.json")
    try:
        with open(path, "r") as f:
            return set(json.load(f))
    except FileNotFoundError:
        return set()

def save_known_ids(ids: Set[str], source: str = "dawum", base_dir: str = "data"):
    """
    Save known IDs to a JSON file.
    
    Args:
        ids: Set of IDs to save
        source: Data source identifier (e.g., 'dawum')
        base_dir: Base directory for data storage
    """
    path = os.path.join(base_dir, source, "known_ids.json")
    ensure_directory_exists(path)
    with open(path, "w") as f:
        json.dump(list(ids), f)

def save_poll_content(poll_id: str, content: Dict[str, Any], source: str = "dawum", base_dir: str = "data"):
    """
    Save poll content to a JSON file.
    
    Args:
        poll_id: ID of the poll
        content: Poll content to save
        source: Data source identifier (e.g., 'dawum')
        base_dir: Base directory for data storage
    """
    # THIS IS WHERE INDIVIDUAL POLL FILES ARE WRITTEN TO DISK
    polls_dir = os.path.join(base_dir, source, "polls")
    path = os.path.join(polls_dir, f"{poll_id}.json")
    ensure_directory_exists(path)
    with open(path, "w") as f:
        json.dump(content, f, indent=2)

def save_multiple_polls(polls: Dict[str, Dict[str, Any]], source: str = "dawum", base_dir: str = "data", limit: int = None):
    """
    Save multiple poll contents to JSON files.
    
    Args:
        polls: Dictionary mapping poll IDs to poll content
        source: Data source identifier (e.g., 'dawum')
        base_dir: Base directory for data storage
        limit: Maximum number of polls to save (None for all)
    """
    # THIS FUNCTION LOOPS THROUGH ALL POLLS AND CALLS save_poll_content FOR EACH
    count = 0
    for poll_id, content in polls.items():
        save_poll_content(poll_id, content, source, base_dir)
        count += 1
        if limit is not None and count >= limit:
            break
    return count

def save_polls_dict(polls: Dict[str, Dict[str, Any]], source: str = "dawum", base_dir: str = "data"):
    """
    Save all polls as a single dictionary in a JSON file.
    
    Args:
        polls: Dictionary mapping poll IDs to poll content
        source: Data source identifier (e.g., 'dawum')
        base_dir: Base directory for data storage
    
    Returns:
        Path to the saved file
    """
    # Create the polls directory if it doesn't exist
    polls_dir = os.path.join(base_dir, source)
    os.makedirs(polls_dir, exist_ok=True)
    
    # Path for the combined polls file
    path = os.path.join(polls_dir, "all_polls.json")
    
    # Save all polls as a single JSON file
    with open(path, "w") as f:
        json.dump(polls, f, indent=2)
    
    return path

def save_api_mapping(mapping_data: Dict[str, Any], mapping_type: str, source: str = "dawum", base_dir: str = "data"):
    """
    Save API mapping data (parties, parliaments, institutes, etc.) to a JSON file.
    
    Args:
        mapping_data: Dictionary containing mapping data
        mapping_type: Type of mapping (e.g., 'Parties', 'Parliaments')
        source: Data source identifier (e.g., 'dawum')
        base_dir: Base directory for data storage
    
    Returns:
        Path to the saved file
    """
    # Create the directory if it doesn't exist
    source_dir = os.path.join(base_dir, source)
    os.makedirs(source_dir, exist_ok=True)
    
    # Path for the mapping file
    path = os.path.join(source_dir, f"{mapping_type.lower()}.json")
    
    # Save the mapping data as a JSON file
    with open(path, "w") as f:
        json.dump(mapping_data, f, indent=2)
    
    return path

def save_all_api_mappings(api_data: Dict[str, Any], source: str = "dawum", base_dir: str = "data"):
    """
    Save all mapping data from the API to separate JSON files.
    
    Args:
        api_data: Dictionary containing all API data
        source: Data source identifier (e.g., 'dawum')
        base_dir: Base directory for data storage
    
    Returns:
        Dictionary mapping the mapping types to their file paths
    """
    saved_paths = {}
    
    # List of mappings to extract and save (excluding Surveys which are handled separately)
    mappings = ["Parties", "Parliaments", "Institutes", "Taskers", "Methods", "Database"]
    
    for mapping_type in mappings:
        if mapping_type in api_data:
            path = save_api_mapping(api_data[mapping_type], mapping_type, source, base_dir)
            saved_paths[mapping_type] = path
    
    return saved_paths
