#!/usr/bin/env python3
"""
Test ClickHouse performance with Parquet files containing Bloom filters
"""

import subprocess
import time
import requests
import json

def wait_for_clickhouse(max_attempts=30):
    """Wait for ClickHouse to be ready"""
    for i in range(max_attempts):
        try:
            response = requests.get('http://localhost:8123/ping', timeout=2)
            if response.status_code == 200:
                print("✅ ClickHouse is ready!")
                return True
        except:
            pass
        print(f"⏳ Waiting for ClickHouse... ({i+1}/{max_attempts})")
        time.sleep(2)
    return False

def execute_query(query, format_type="JSON"):
    """Execute a query on ClickHouse"""
    url = f"http://localhost:8123/?query={query}&default_format={format_type}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
        else:
            print(f"❌ Query failed: {response.text}")
            return None
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return None

def test_parquet_files():
    """Test both Parquet files with ClickHouse"""
    print("\n🔍 Testing Parquet files with ClickHouse...")
    
    # Test file with Bloom filters
    print("\n📊 Testing file WITH Bloom filters:")
    query_bloom = "SELECT COUNT(*) as total_rows FROM file('test_bloom.parquet', 'Parquet')"
    result = execute_query(query_bloom)
    if result:
        print(f"   Rows: {json.loads(result)['data'][0]['total_rows']}")
    
    # Test file without Bloom filters  
    print("\n📊 Testing file WITHOUT Bloom filters:")
    query_no_bloom = "SELECT COUNT(*) as total_rows FROM file('test_no_bloom.parquet', 'Parquet')"
    result = execute_query(query_no_bloom)
    if result:
        print(f"   Rows: {json.loads(result)['data'][0]['total_rows']}")
    
    # Test schema
    print("\n📋 Schema information:")
    schema_query = "DESCRIBE file('test_bloom.parquet', 'Parquet')"
    result = execute_query(schema_query)
    if result:
        data = json.loads(result)
        for row in data['data']:
            print(f"   {row['name']:12s} {row['type']:20s}")
    
    # Performance test - filter by distinct_id (should use Bloom filter)
    print("\n🚀 Performance test - Filter by distinct_id:")
    
    # Get a sample distinct_id first
    sample_query = "SELECT distinct_id FROM file('test_bloom.parquet', 'Parquet') LIMIT 1"
    result = execute_query(sample_query)
    if result:
        sample_id = json.loads(result)['data'][0]['distinct_id']
        print(f"   Using sample distinct_id: {sample_id[:16]}...")
        
        # Test with Bloom filter file
        print("   🌸 WITH Bloom filters:")
        start_time = time.time()
        filter_query_bloom = f"SELECT COUNT(*) as count FROM file('test_bloom.parquet', 'Parquet') WHERE distinct_id = '{sample_id}'"
        result = execute_query(filter_query_bloom)
        bloom_time = time.time() - start_time
        if result:
            count = json.loads(result)['data'][0]['count']
            print(f"      Found {count} rows in {bloom_time:.4f}s")
        
        # Test without Bloom filter file
        print("   📄 WITHOUT Bloom filters:")
        start_time = time.time()
        filter_query_no_bloom = f"SELECT COUNT(*) as count FROM file('test_no_bloom.parquet', 'Parquet') WHERE distinct_id = '{sample_id}'"
        result = execute_query(filter_query_no_bloom)
        no_bloom_time = time.time() - start_time
        if result:
            count = json.loads(result)['data'][0]['count']
            print(f"      Found {count} rows in {no_bloom_time:.4f}s")
        
        if bloom_time > 0 and no_bloom_time > 0:
            speedup = no_bloom_time / bloom_time
            print(f"   📈 Bloom filter speedup: {speedup:.2f}x")

def main():
    print("🐘 Starting ClickHouse Parquet Test")
    print("=" * 50)
    
    if wait_for_clickhouse():
        test_parquet_files()
    else:
        print("❌ Could not connect to ClickHouse")

if __name__ == "__main__":
    main()