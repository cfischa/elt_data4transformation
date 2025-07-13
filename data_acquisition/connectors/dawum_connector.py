"""
DawumConnector
--------------
Inputs:   Optional query params (dict) for the DAWUM API.
Outputs:  Raw JSON from the DAWUM polling API.
Functionality:
    - Handles API connection to dawum.de (https://dawum.de/API/)
    - Fetches polling data with optional query parameters.
    - Logs requests and handles HTTP errors.
"""

import requests
import logging
from typing import Optional

class DawumConnector:
    BASE_URL = "https://api.dawum.de"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()
        self.logger = logging.getLogger("dawum_connector")

    def fetch_polls(self):
        url = f"{self.BASE_URL}/"    # ACHTUNG: nur ein Slash!
        self.logger.info(f"Fetching from {url}")
        try:
            resp = self.session.get(url)
            resp.raise_for_status()
            data = resp.json()
            self.logger.info("Data fetch successful")
            return data
        except Exception as e:
            self.logger.error(f"Error fetching data: {e}")
            raise
