#!/usr/bin/env python3
"""Check ClickHouse database structure"""


import os
import clickhouse_connect

def main():
    # Load credentials from environment variables
    host = os.getenv('CLICKHOUSE_HOST', 'localhost')
    port = int(os.getenv('CLICKHOUSE_PORT', '8124'))
    username = os.getenv('CLICKHOUSE_USER', '')
    password = os.getenv('CLICKHOUSE_PASSWORD', '')
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password
    )
    
    print("=== ClickHouse Database Information ===\n")
    
    print("Available databases:")
    result = client.query('SHOW DATABASES')
    for row in result.result_rows:
        print(f"  - {row[0]}")
    
    print(f"\nClickHouse version: {client.query('SELECT version()').result_rows[0][0]}")
    print(f"Current user: {client.query('SELECT currentUser()').result_rows[0][0]}")

if __name__ == "__main__":
    main()
