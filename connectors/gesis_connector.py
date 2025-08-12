"""
GESIS API connector for social science data.
Implements the GESIS Data Catalogue API for accessing social science research data.
"""

import os
from datetime import datetime
from typing import Dict, Any, AsyncGenerator, Optional, List
from pydantic import BaseModel
from SPARQLWrapper import SPARQLWrapper, JSON

from .base_connector import BaseConnector, ConnectorConfig


class GESISConfig(ConnectorConfig):
    """Configuration for GESIS API connector."""
    
    def __init__(self):
        super().__init__(
            base_url="https://search.gesis.org/research_data",
            api_key=os.getenv("GESIS_API_KEY"),
            rate_limit_requests=60,
            rate_limit_period=60,
        )


class GESISConnector(BaseConnector):

    async def fetch_data(self, *args, **kwargs):
        """Stub implementation to satisfy abstract base class. Not implemented."""
        raise NotImplementedError("fetch_data is not implemented in this connector version.")

    async def get_incremental_data(self, *args, **kwargs):
        """Stub implementation to satisfy abstract base class. Not implemented."""
        raise NotImplementedError("get_incremental_data is not implemented in this connector version.")
    """
    Connector for GESIS - Leibniz Institute for the Social Sciences.
    
    Provides methods to list available datasets and fetch relevant metadata for a given dataset.
    """

    def __init__(self, config: Optional[GESISConfig] = None):
        self.config = config or GESISConfig()
        super().__init__(self.config)

    async def list_datasets(self) -> List[Dict[str, Any]]:
        """
        List all available datasets (schema:Dataset) from the GESIS Knowledge Graph.
        Uses CONSTRUCT query to get all RDF triples for datasets.
        """
        import asyncio
        sparql_url = "https://data.gesis.org/gesiskg/sparql"
        
        sparql = SPARQLWrapper(sparql_url)
        
        # First get a simple list of dataset URIs
        list_query = '''
        PREFIX schema: <https://schema.org/>
        SELECT DISTINCT ?d WHERE {
          ?d a schema:Dataset .
        }
        '''
        
        sparql.setQuery(list_query)
        sparql.setReturnFormat(JSON)
        
        try:
            results = await asyncio.to_thread(lambda: sparql.query().convert())
            bindings = results["results"]["bindings"]
            
            datasets = []
            for binding in bindings:
                dataset_uri = binding["d"]["value"]
                datasets.append({
                    "id": dataset_uri,
                    "type": "https://schema.org/Dataset",
                    "title": ""  # Will be filled by get_metadata
                })
            
            self.logger.info(f"Found {len(datasets)} datasets")
            return datasets
            
        except Exception as e:
            self.logger.error(f"SPARQL error fetching dataset list: {e}")
            return []

    async def get_metadata(self, resource_id: str) -> Dict[str, Any]:
        """
        Fetch all RDF triples for a specific dataset using CONSTRUCT query.
        Returns a dict with all available metadata.
        """
        import asyncio
        sparql_url = "https://data.gesis.org/gesiskg/sparql"
        sparql = SPARQLWrapper(sparql_url)
        
        # Use CONSTRUCT query to get all triples for this dataset
        construct_query = f'''
        PREFIX schema: <https://schema.org/>
        
        CONSTRUCT {{
          <{resource_id}> ?p ?o .
        }}
        WHERE {{
          <{resource_id}> a schema:Dataset ;
             ?p ?o .
        }}
        '''
        
        sparql.setQuery(construct_query)
        sparql.setReturnFormat(JSON)
        
        try:
            results = await asyncio.to_thread(lambda: sparql.query().convert())
            
            # Process the CONSTRUCT result
            metadata = {
                "id": resource_id,
                "type": "https://schema.org/Dataset",
                "title": "",
                "description": "",
                "creator": "",
                "issued": "",
                "variables": [],
                "status": "success",
                "properties": {}
            }
            
            # Extract triples from the result
            if "@graph" in results:
                # JSON-LD format
                for triple in results["@graph"]:
                    if "@id" in triple and triple["@id"] == resource_id:
                        for prop, value in triple.items():
                            if prop.startswith("@"):
                                continue
                            metadata["properties"][prop] = value
                            
                            # Map common properties
                            if "title" in prop.lower():
                                metadata["title"] = str(value) if not isinstance(value, list) else str(value[0])
                            elif "abstract" in prop.lower() or "description" in prop.lower():
                                metadata["description"] = str(value) if not isinstance(value, list) else str(value[0])
                            elif "creator" in prop.lower():
                                metadata["creator"] = str(value) if not isinstance(value, list) else str(value[0])
                            elif "issued" in prop.lower() or "date" in prop.lower():
                                metadata["issued"] = str(value) if not isinstance(value, list) else str(value[0])
            
            elif "results" in results and "bindings" in results["results"]:
                # Standard SPARQL JSON format
                for binding in results["results"]["bindings"]:
                    prop = binding.get("p", {}).get("value", "")
                    obj = binding.get("o", {}).get("value", "")
                    
                    if prop and obj:
                        # Store all properties
                        prop_short = prop.split("/")[-1].split("#")[-1]
                        metadata["properties"][prop_short] = obj
                        
                        # Map common properties
                        if "title" in prop_short.lower():
                            metadata["title"] = obj
                        elif "abstract" in prop_short.lower() or "description" in prop_short.lower():
                            metadata["description"] = obj
                        elif "creator" in prop_short.lower():
                            metadata["creator"] = obj
                        elif "issued" in prop_short.lower() or "date" in prop_short.lower():
                            metadata["issued"] = obj
                        elif "variable" in prop_short.lower():
                            if obj not in metadata["variables"]:
                                metadata["variables"].append(obj)
            
            # Ensure we have some basic info
            if not metadata["title"] and resource_id:
                metadata["title"] = f"Dataset {resource_id.split('/')[-1]}"
            
            self.logger.info(f"Retrieved {len(metadata['properties'])} properties for {resource_id}")
            return metadata
            
        except Exception as e:
            self.logger.error(f"SPARQL error fetching metadata for {resource_id}: {e}")
            return {
                "id": resource_id,
                "type": "https://schema.org/Dataset", 
                "error": str(e),
                "status": "error",
                "title": "",
                "description": "",
                "creator": "",
                "issued": "",
                "variables": [],
                "properties": {}
            }

    async def get_all_datasets_metadata(self, batch_size: int = 50, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get metadata for datasets using SELECT queries with proper pagination.
        Uses smaller batches to avoid URI length limits on the SPARQL endpoint.
        """
        import asyncio
        from SPARQLWrapper import SPARQLWrapper, JSON
        
        sparql_url = "https://data.gesis.org/gesiskg/sparql"
        
        # First get list of all dataset URIs (with optional limit)
        sparql = SPARQLWrapper(sparql_url)
        
        # Build the list query with optional LIMIT
        list_query = '''
        PREFIX schema: <https://schema.org/>
        SELECT DISTINCT ?d WHERE {
          ?d a schema:Dataset .
        }
        '''
        
        if limit:
            list_query += f' LIMIT {limit}'
        
        sparql.setQuery(list_query)
        sparql.setReturnFormat(JSON)
        
        try:
            results = await asyncio.to_thread(lambda: sparql.query().convert())
            dataset_uris = [binding["d"]["value"] for binding in results["results"]["bindings"]]
            
            self.logger.info(f"Found {len(dataset_uris)} datasets to process")
            
            # Process datasets in smaller batches to avoid URI length limits
            # For large datasets, use very small batch sizes (20-30 URIs max)
            effective_batch_size = min(batch_size, 20) if len(dataset_uris) > 100 else min(batch_size, 50)
            
            all_metadata = []
            for i in range(0, len(dataset_uris), effective_batch_size):
                batch_uris = dataset_uris[i:i + effective_batch_size]
                
                # Create a SELECT query for this batch
                uris_filter = " ".join([f"<{uri}>" for uri in batch_uris])
                
                batch_query = f'''
                PREFIX schema: <https://schema.org/>
                PREFIX dct: <http://purl.org/dc/terms/>
                PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                PREFIX dcat: <http://www.w3.org/ns/dcat#>
                
                SELECT ?d ?p ?o WHERE {{
                  VALUES ?d {{ {uris_filter} }}
                  ?d a schema:Dataset ;
                     ?p ?o .
                }}
                '''
                
                sparql.setQuery(batch_query)
                sparql.setReturnFormat(JSON)
                
                try:
                    batch_results = await asyncio.to_thread(lambda: sparql.query().convert())
                    
                    # Group results by dataset
                    datasets_data = {}
                    for binding in batch_results["results"]["bindings"]:
                        dataset_uri = binding["d"]["value"]
                        prop = binding["p"]["value"]
                        obj_data = binding["o"]
                        
                        if dataset_uri not in datasets_data:
                            datasets_data[dataset_uri] = {
                                "id": dataset_uri,
                                "type": "https://schema.org/Dataset",
                                "title": "",
                                "description": "",
                                "creator": "",
                                "issued": "",
                                "variables": [],
                                "status": "success",
                                "properties": {},
                                "raw_data": ""
                            }
                        
                        # Store the property
                        prop_short = prop.split("/")[-1].split("#")[-1]
                        obj_value = obj_data.get("value", "")
                        
                        datasets_data[dataset_uri]["properties"][prop_short] = obj_value
                        
                        # Add to raw_data for ClickHouse
                        datasets_data[dataset_uri]["raw_data"] += f"{prop_short}: {obj_value}\\n"
                        
                        # Map common properties to standard fields
                        if "title" in prop_short.lower() or "name" in prop_short.lower():
                            if not datasets_data[dataset_uri]["title"]:
                                datasets_data[dataset_uri]["title"] = obj_value
                        elif "abstract" in prop_short.lower() or "description" in prop_short.lower():
                            if not datasets_data[dataset_uri]["description"]:
                                datasets_data[dataset_uri]["description"] = obj_value
                        elif "creator" in prop_short.lower() or "author" in prop_short.lower():
                            if not datasets_data[dataset_uri]["creator"]:
                                datasets_data[dataset_uri]["creator"] = obj_value
                        elif "issued" in prop_short.lower() or "date" in prop_short.lower():
                            if not datasets_data[dataset_uri]["issued"]:
                                datasets_data[dataset_uri]["issued"] = obj_value
                        elif "variable" in prop_short.lower():
                            if obj_value not in datasets_data[dataset_uri]["variables"]:
                                datasets_data[dataset_uri]["variables"].append(obj_value)
                    
                    # Add batch to results
                    batch_metadata = list(datasets_data.values())
                    all_metadata.extend(batch_metadata)
                    
                    self.logger.info(f"Processed batch {i//effective_batch_size + 1}/{(len(dataset_uris) + effective_batch_size - 1)//effective_batch_size}: {len(batch_metadata)} datasets")
                    
                except Exception as batch_error:
                    self.logger.warning(f"Batch {i//effective_batch_size + 1} failed: {batch_error}")
                    # Continue processing other batches
                    continue
            
            self.logger.info(f"Retrieved metadata for {len(all_metadata)} datasets total")
            return all_metadata
            
        except Exception as e:
            self.logger.error(f"SPARQL error in batch metadata fetch: {e}")
            import traceback
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            return []

    async def get_limited_datasets_metadata(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get metadata for a limited number of datasets (for testing).
        """
        return await self.get_all_datasets_metadata(batch_size=50, limit=limit)


# Example usage: Fetch all metadata using CONSTRUCT queries
if __name__ == "__main__":
    import asyncio
    import json
    from collections import OrderedDict

    async def main():
        async with GESISConnector() as connector:
            # Step 1: List all resources
            datasets = await connector.list_datasets()
            print(f"Available resources: {len(datasets)}")
            if not datasets:
                print("No resources found.")
                return

            # Step 2: Fetch metadata for each resource
            all_metadata = []
            seen_ids = set()
            for i, ds in enumerate(datasets):
                rid = ds["id"]
                if rid in seen_ids:
                    continue
                seen_ids.add(rid)
                meta = await connector.get_metadata(rid)
                all_metadata.append(meta)
                if (i+1) % 10 == 0:
                    print(f"Fetched metadata for {i+1} resources...")

            # Step 3: Deduplicate by id (already done above)

            # Step 4: Save to JSON file

            out_path = "meta_data/gesis_metadata.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(all_metadata, f, ensure_ascii=False, indent=2)
            print(f"Saved metadata for {len(all_metadata)} resources to {out_path}")

            # Optional: Print a sample for review
            print("Sample metadata entry:")
            print(json.dumps(all_metadata[0], indent=2, ensure_ascii=False))

    asyncio.run(main())
