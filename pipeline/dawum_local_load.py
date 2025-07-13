"""
DAWUM local data loading script.
CLI convenience script for loading DAWUM data outside of Airflow.
"""

import asyncio
import logging
import sys
import os
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.logging import RichHandler

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from connectors.dawum_connector import DawumConnector, DawumConfig
from elt.loader_clickhouse import ClickHouseLoader, ClickHouseConfig

app = typer.Typer(help="DAWUM local data loader")
console = Console()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, rich_tracebacks=True)]
)
logger = logging.getLogger(__name__)


@app.command()
def load(
    limit: Optional[int] = typer.Option(None, "--limit", "-l", help="Maximum number of polls to fetch"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Extract data but don't load to database"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging")
):
    """Load DAWUM polling data into ClickHouse."""
    
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    console.print("[bold blue]DAWUM Local Data Loader[/bold blue]")
    console.print(f"Limit: {limit or 'No limit'}")
    console.print(f"Dry run: {dry_run}")
    console.print()
    
    try:
        asyncio.run(_load_data(limit, dry_run))
    except KeyboardInterrupt:
        console.print("\n[yellow]Operation cancelled by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        sys.exit(1)


async def _load_data(limit: Optional[int], dry_run: bool):
    """Async function to load DAWUM data."""
    
    # Initialize DAWUM connector
    dawum_config = DawumConfig(
        base_url=os.getenv("DAWUM_BASE_URL", "https://api.dawum.de"),
        api_key=os.getenv("DAWUM_API_KEY"),
        rate_limit_requests=60,
        rate_limit_period=60,
        timeout=30,
        max_retries=3
    )
    
    connector = DawumConnector(dawum_config)
    
    # Extract data
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        
        extract_task = progress.add_task("Extracting DAWUM polls...", total=None)
        
        try:
            polls = await connector.fetch_polls(limit)
            progress.update(extract_task, description=f"✅ Extracted {len(polls)} polls")
            
        except Exception as e:
            progress.update(extract_task, description="❌ Extraction failed")
            raise
    
    console.print(f"\n[green]Successfully extracted {len(polls)} polls[/green]")
    
    if not polls:
        console.print("[yellow]No data to load[/yellow]")
        return
    
    # Display sample data
    if polls:
        console.print("\n[bold]Sample poll data:[/bold]")
        sample = polls[0]
        for key, value in list(sample.items())[:5]:
            console.print(f"  {key}: {value}")
        console.print("  ...")
    
    if dry_run:
        console.print("\n[yellow]Dry run mode - skipping database load[/yellow]")
        return
    
    # Load to ClickHouse
    clickhouse_config = ClickHouseConfig(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "analytics")
    )
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        
        load_task = progress.add_task("Loading to ClickHouse...", total=None)
        
        try:
            with ClickHouseLoader(clickhouse_config) as loader:
                rows_loaded = loader.upsert_dawum_polls(polls)
                progress.update(load_task, description=f"✅ Loaded {rows_loaded} rows")
                
        except Exception as e:
            progress.update(load_task, description="❌ Load failed")
            raise
    
    console.print(f"\n[green]Successfully loaded {rows_loaded} rows to ClickHouse[/green]")
    console.print(f"Target table: [bold]raw.dawum_polls[/bold]")


@app.command()
def test_connection():
    """Test connections to DAWUM API and ClickHouse."""
    
    console.print("[bold blue]Testing Connections[/bold blue]")
    
    # Test DAWUM API
    console.print("\n🔗 Testing DAWUM API connection...")
    try:
        import aiohttp
        async def test_dawum():
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                base_url = os.getenv("DAWUM_BASE_URL", "https://api.dawum.de")
                async with session.get(f"{base_url}/parties") as response:
                    response.raise_for_status()
                    return await response.json()
        
        result = asyncio.run(test_dawum())
        console.print("✅ DAWUM API connection successful")
        
    except Exception as e:
        console.print(f"❌ DAWUM API connection failed: {e}")
    
    # Test ClickHouse
    console.print("\n🗄️  Testing ClickHouse connection...")
    try:
        clickhouse_config = ClickHouseConfig(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_DATABASE", "analytics")
        )
        
        with ClickHouseLoader(clickhouse_config) as loader:
            loader.client.ping()
            console.print("✅ ClickHouse connection successful")
            
    except Exception as e:
        console.print(f"❌ ClickHouse connection failed: {e}")


if __name__ == "__main__":
    app()
