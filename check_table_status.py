from elt.loader_clickhouse import ClickHouseLoader

with ClickHouseLoader() as ch:
    # Check if table exists
    result = ch.client.query("SHOW TABLES FROM raw LIKE 'destatis_metadata'")
    if result.result_rows:
        print('✅ Table destatis_metadata exists')
        count = ch.client.query('SELECT COUNT(*) FROM raw.destatis_metadata')
        print(f'📊 Current records: {count.result_rows[0][0]}')
        
        # Show table structure
        structure = ch.client.query('DESCRIBE raw.destatis_metadata')
        print('📋 Table structure:')
        for row in structure.result_rows:
            print(f'  {row[0]} - {row[1]}')
    else:
        print('❌ Table does not exist yet')
