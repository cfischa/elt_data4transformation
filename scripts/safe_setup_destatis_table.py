#!/usr/bin/env python3
"""
SAFE ClickHouse table setup for Destatis metadata.
NEVER drops existing data - only creates if table doesn't exist.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from elt.loader_clickhouse import ClickHouseLoader

def safely_create_destatis_metadata_table():
    """
    SAFELY create the destatis_metadata table - NEVER drops existing data.
    """
    
    try:
        with ClickHouseLoader() as ch_loader:
            print("🔍 Checking ClickHouse table status...")
            
            # Check if table already exists
            result = ch_loader.client.query("SHOW TABLES FROM raw LIKE 'destatis_metadata'")
            table_exists = len(result.result_rows) > 0
            
            if table_exists:
                print("⚠️  Table 'raw.destatis_metadata' ALREADY EXISTS")
                
                # Check if it has data
                count = ch_loader.client.query("SELECT COUNT(*) FROM raw.destatis_metadata")
                row_count = count.result_rows[0][0] if count.result_rows else 0
                
                if row_count > 0:
                    print(f"💾 Table contains {row_count} existing records")
                    print("✅ SAFE: Will NOT modify existing table with data")
                    return True
                else:
                    print("📊 Table exists but is empty")
                    print("✅ SAFE: Table ready for new data")
                    return True
            
            # Table doesn't exist - safe to create
            print("🔧 Creating new table (safe - no existing data)...")
            
            # Create database if needed
            ch_loader.client.command("CREATE DATABASE IF NOT EXISTS raw")
            print("✅ Database 'raw' ready")
            
            # Create table with safe schema
            create_table_sql = """
            CREATE TABLE raw.destatis_metadata (
                cube_code String,
                content String DEFAULT '',
                state String DEFAULT '',
                time_coverage String DEFAULT '',
                latest_update Nullable(DateTime),
                information Nullable(Bool),
                fetched_at DateTime,
                source String DEFAULT 'destatis_catalogue',
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now(),
                event_date Date MATERIALIZED toDate(fetched_at)
            ) ENGINE = ReplacingMergeTree(updated_at)
            PARTITION BY toYYYYMM(event_date)
            ORDER BY (cube_code, fetched_at)
            SETTINGS index_granularity = 8192
            """
            
            ch_loader.client.command(create_table_sql)
            print("✅ Table 'raw.destatis_metadata' created successfully")
            
            # Add indexes safely
            try:
                ch_loader.client.command(
                    "ALTER TABLE raw.destatis_metadata ADD INDEX IF NOT EXISTS idx_cube_code cube_code TYPE minmax GRANULARITY 1"
                )
                ch_loader.client.command(
                    "ALTER TABLE raw.destatis_metadata ADD INDEX IF NOT EXISTS idx_content content TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1"
                )
                print("✅ Indexes added successfully")
            except Exception as e:
                print(f"⚠️  Index creation warning (non-critical): {e}")
            
            # Verify table structure
            structure = ch_loader.client.query("DESCRIBE raw.destatis_metadata")
            print("\n📋 Table structure:")
            for row in structure.result_rows:
                print(f"  {row[0]} - {row[1]}")
                
            return True
                
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def check_existing_data():
    """Check what data exists before any operations."""
    try:
        with ClickHouseLoader() as ch_loader:
            print("🔍 DATA SAFETY CHECK:")
            
            # Check all tables in raw database
            tables = ch_loader.client.query("SHOW TABLES FROM raw")
            
            if not tables.result_rows:
                print("✅ No tables in 'raw' database - safe to proceed")
                return True
            
            print(f"📊 Found {len(tables.result_rows)} existing tables:")
            total_records = 0
            
            for table_row in tables.result_rows:
                table_name = table_row[0]
                try:
                    count = ch_loader.client.query(f'SELECT COUNT(*) FROM raw.{table_name}')
                    row_count = count.result_rows[0][0] if count.result_rows else 0
                    total_records += row_count
                    print(f"  📋 {table_name}: {row_count:,} rows")
                except Exception as e:
                    print(f"  ⚠️  {table_name}: Could not count rows - {e}")
            
            if total_records > 0:
                print(f"\n⚠️  TOTAL EXISTING DATA: {total_records:,} records")
                print("🛡️  SAFETY PROTOCOL: Will NOT drop any existing data")
            
            return True
            
    except Exception as e:
        print(f"❌ Safety check failed: {e}")
        return False

if __name__ == "__main__":
    print("🛡️  SAFE DESTATIS METADATA TABLE SETUP")
    print("=" * 50)
    
    # First, check existing data
    if not check_existing_data():
        print("💥 Safety check failed. Aborting.")
        sys.exit(1)
    
    print("\n" + "=" * 50)
    
    # Then safely create table
    success = safely_create_destatis_metadata_table()
    
    if success:
        print("\n🎯 SUCCESS: Table ready for Destatis metadata!")
        print("📍 Location: raw.destatis_metadata")
        print("🔒 Data safety: All existing data preserved")
    else:
        print("\n💥 FAILED: Please check ClickHouse connection")
        sys.exit(1)
