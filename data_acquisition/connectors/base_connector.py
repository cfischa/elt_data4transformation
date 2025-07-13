# Inside base_connector.py
class BaseConnector:
    def __init__(self, source_name):
        self.source_name = source_name
    
    def fetch_data(self):
        """Must be implemented by subclasses"""
        raise NotImplementedError