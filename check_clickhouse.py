#!/usr/bin/env python3
"""Check ClickHouse database structure"""

import clickhouse_connect

def main():
    client = clickhouse_connect.get_client(
        host='localhost', 
        port=8124, 
        username='admin', 
        password='asjrh25423sfa#+43qw56j'
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
