# Inside connectors/__init__.py
from .dawum_connector import DawumConnector
#from .eurostat_connector import EurostatConnector
# Add more imports as you create them

# Registry of available connectors
CONNECTOR_REGISTRY = {
    "dawum": DawumConnector,
    #"eurostat": EurostatConnector,
    # Add more here
}

def get_connector(source_name):
    """Factory function to get the appropriate connector"""
    connector_class = CONNECTOR_REGISTRY.get(source_name)
    if not connector_class:
        raise ValueError(f"No connector available for source: {source_name}")
    return connector_class()