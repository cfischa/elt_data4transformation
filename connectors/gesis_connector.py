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
        List all available datasets (schema:Dataset) from the GESIS Knowledge Graph using paging.
        Returns a list of dicts with minimal metadata: id (IRI), title, and type.
        """
        import asyncio
        sparql_url = "https://data.gesis.org/gesiskg/sparql"
        batch_size = 500
        offset = 0
        all_results = []
        while True:
            sparql = SPARQLWrapper(sparql_url)
            query = f'''
            PREFIX dct: <http://purl.org/dc/terms/>
            PREFIX schema: <https://schema.org/>
            SELECT ?resource ?type ?title WHERE {{
              ?resource a schema:Dataset .
              BIND(schema:Dataset AS ?type)
              OPTIONAL {{ ?resource dct:title ?title }}
            }} OFFSET {offset} LIMIT {batch_size}
            '''
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            try:
                results = await asyncio.to_thread(lambda: sparql.query().convert())
                bindings = results["results"]["bindings"]
                if not bindings:
                    break
                for r in bindings:
                    all_results.append({
                        "id": r["resource"]["value"],
                        "type": r["type"]["value"],
                        "title": r.get("title", {}).get("value", ""),
                    })
                print(f"Fetched {len(all_results)} datasets so far...")
                offset += batch_size
            except Exception as e:
                self.logger.error(f"SPARQL error fetching dataset list: {e}")
                break
        return all_results

    async def get_metadata(self, resource_id: str) -> Dict[str, Any]:
        """
        Fetch and return all relevant metadata fields for a GESIS resource (Dataset, Variable, Instrument, ScholarlyArticle).
        Returns a dict with fields relevant to the resource type.
        """
        import asyncio
        sparql = SPARQLWrapper("https://data.gesis.org/gesiskg/sparql")
        # Query for type first
        type_query = f'''
        SELECT ?type WHERE {{
          <{resource_id}> a ?type .
        }} LIMIT 1
        '''
        sparql.setQuery(type_query)
        sparql.setReturnFormat(JSON)
        try:
            type_result = await asyncio.to_thread(lambda: sparql.query().convert())
            type_bindings = type_result["results"]["bindings"]
            if not type_bindings:
                return {"id": resource_id, "error": "Resource not found", "status": "error"}
            resource_type = type_bindings[0]["type"]["value"]
        except Exception as e:
            self.logger.error(f"SPARQL error fetching resource type for {resource_id}: {e}")
            return {"id": resource_id, "error": str(e), "status": "error"}

        # Now query for relevant fields based on type
        if resource_type.endswith("Dataset"):
            query = f'''
            PREFIX dct: <http://purl.org/dc/terms/>
            PREFIX disco: <http://rdf-vocabulary.ddialliance.org/discovery#>
            SELECT ?title ?abstract ?issued ?creator ?variable WHERE {{
              BIND(<{resource_id}> AS ?study)
              OPTIONAL {{ ?study dct:title ?title }}
              OPTIONAL {{ ?study dct:abstract ?abstract }}
              OPTIONAL {{ ?study dct:issued ?issued }}
              OPTIONAL {{ ?study dct:creator ?creator }}
              OPTIONAL {{ ?study disco:variable ?variable }}
            }}
            '''
        elif resource_type.endswith("Variable"):
            query = f'''
            PREFIX dct: <http://purl.org/dc/terms/>
            SELECT ?title ?description ?creator WHERE {{
              BIND(<{resource_id}> AS ?var)
              OPTIONAL {{ ?var dct:title ?title }}
              OPTIONAL {{ ?var dct:description ?description }}
              OPTIONAL {{ ?var dct:creator ?creator }}
            }}
            '''
        elif resource_type.endswith("Instrument"):
            query = f'''
            PREFIX dct: <http://purl.org/dc/terms/>
            SELECT ?title ?description ?creator WHERE {{
              BIND(<{resource_id}> AS ?inst)
              OPTIONAL {{ ?inst dct:title ?title }}
              OPTIONAL {{ ?inst dct:description ?description }}
              OPTIONAL {{ ?inst dct:creator ?creator }}
            }}
            '''
        elif resource_type.endswith("ScholarlyArticle"):
            query = f'''
            PREFIX dct: <http://purl.org/dc/terms/>
            SELECT ?title ?abstract ?issued ?creator WHERE {{
              BIND(<{resource_id}> AS ?art)
              OPTIONAL {{ ?art dct:title ?title }}
              OPTIONAL {{ ?art dct:abstract ?abstract }}
              OPTIONAL {{ ?art dct:issued ?issued }}
              OPTIONAL {{ ?art dct:creator ?creator }}
            }}
            '''
        else:
            return {"id": resource_id, "error": f"Unknown or unsupported resource type: {resource_type}", "status": "error"}

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        try:
            results = await asyncio.to_thread(lambda: sparql.query().convert())
            bindings = results["results"]["bindings"]
            if not bindings:
                return {"id": resource_id, "error": "No metadata found", "status": "error"}
            meta = bindings[0]
            # Build result dict based on type
            result = {"id": resource_id, "type": resource_type}
            if resource_type.endswith("Dataset"):
                variables = [b["variable"]["value"] for b in bindings if "variable" in b]
                result.update({
                    "title": meta.get("title", {}).get("value", ""),
                    "description": meta.get("abstract", {}).get("value", ""),
                    "creator": meta.get("creator", {}).get("value", ""),
                    "issued": meta.get("issued", {}).get("value", ""),
                    "variables": variables,
                })
            elif resource_type.endswith("Variable") or resource_type.endswith("Instrument"):
                result.update({
                    "title": meta.get("title", {}).get("value", ""),
                    "description": meta.get("description", {}).get("value", ""),
                    "creator": meta.get("creator", {}).get("value", ""),
                })
            elif resource_type.endswith("ScholarlyArticle"):
                result.update({
                    "title": meta.get("title", {}).get("value", ""),
                    "description": meta.get("abstract", {}).get("value", ""),
                    "creator": meta.get("creator", {}).get("value", ""),
                    "issued": meta.get("issued", {}).get("value", ""),
                })
            return result
        except Exception as e:
            self.logger.error(f"SPARQL error fetching metadata for {resource_id}: {e}")
            return {"id": resource_id, "error": str(e), "status": "error"}


# Example usage: Fetch all metadata, deduplicate, and save to JSON
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
