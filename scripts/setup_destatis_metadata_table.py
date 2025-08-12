#!/usr/bin/env python3
"""
Initialize ClickHouse table for Destatis metadata.
Run this before executing the fetch_destatis_metadata DAG.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from elt.loader_clickhouse import ClickHouseLoader

def create_destatis_metadata_table():
    """Create the destatis_metadata table in ClickHouse."""
    
    # Read the SQL file
    sql_file = os.path.join(os.path.dirname(__file__), "..", "sql", "create_destatis_metadata_table.sql")
    
    with open(sql_file, 'r') as f:
        sql_commands = f.read()
    
    # Split by semicolon and execute each command
    commands = [cmd.strip() for cmd in sql_commands.split(';') if cmd.strip()]
    
    try:
        with ClickHouseLoader() as ch_loader:
            print("🔧 Creating ClickHouse table for Destatis metadata...")
            
            for i, command in enumerate(commands):
                if command:
                    print(f"📝 Executing command {i+1}/{len(commands)}...")
                    ch_loader.client.command(command)
                    print(f"✅ Command {i+1} executed successfully")
            
            # Verify table creation
            result = ch_loader.client.query("SHOW TABLES FROM raw LIKE 'destatis_metadata'")
            if result.result_rows:
                print("✅ Table 'raw.destatis_metadata' created successfully!")
                
                # Show table structure
                structure = ch_loader.client.query("DESCRIBE raw.destatis_metadata")
                print("\n📋 Table structure:")
                for row in structure.result_rows:
                    print(f"  {row[0]} - {row[1]}")
                    
            else:
                print("❌ Table creation failed - table not found")
                return False
                
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = create_destatis_metadata_table()
    if success:
        print("\n🎯 Ready to run the fetch_destatis_metadata DAG!")
        print("   Data will be saved to: raw.destatis_metadata")
    else:
        print("\n💥 Setup failed. Please check ClickHouse connection and try again.")
        sys.exit(1)
