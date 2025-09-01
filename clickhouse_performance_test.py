#!/usr/bin/env python3
"""
Comprehensive ClickHouse performance test for Bloom filters
"""

import time
import statistics
from clickhouse_driver import Client

# Initialize ClickHouse client
client = Client(host='localhost', port=9000)

def run_clickhouse_query(query):
    """Run a query in ClickHouse and return execution time"""
    start_time = time.time()
    try:
        result = client.execute(query)
        execution_time = time.time() - start_time
        return execution_time, result
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return None, None

def performance_test(test_name, query_bloom, query_no_bloom, iterations=5):
    """Run a performance test comparing Bloom vs non-Bloom files"""
    print(f"\n🔬 {test_name}")
    print("=" * len(test_name))
    
    bloom_times = []
    no_bloom_times = []
    
    for i in range(iterations):
        print(f"   Run {i+1}/{iterations}...")
        
        # Test with Bloom filters
        time_bloom, result_bloom = run_clickhouse_query(query_bloom)
        if time_bloom is not None:
            bloom_times.append(time_bloom)
        
        # Test without Bloom filters  
        time_no_bloom, result_no_bloom = run_clickhouse_query(query_no_bloom)
        if time_no_bloom is not None:
            no_bloom_times.append(time_no_bloom)
    
    if bloom_times and no_bloom_times:
        avg_bloom = statistics.mean(bloom_times)
        avg_no_bloom = statistics.mean(no_bloom_times)
        speedup = avg_no_bloom / avg_bloom
        
        print(f"   🌸 WITH Bloom filters:    {avg_bloom:.4f}s (avg)")
        print(f"   📄 WITHOUT Bloom filters: {avg_no_bloom:.4f}s (avg)")
        print(f"   📈 Bloom filter speedup:  {speedup:.2f}x")
        
        if speedup > 1.1:
            print("   ✅ Significant improvement with Bloom filters!")
        elif speedup > 1.0:
            print("   ⚠️  Modest improvement with Bloom filters")
        else:
            print("   ❌ No improvement with Bloom filters")
        
        return speedup
    else:
        print("   ❌ Test failed - could not collect timing data")
        return None

def main():
    print("🚀 ClickHouse Bloom Filter Performance Analysis")
    print("=" * 50)
    
    # Test 1: Non-existent distinct_id (should be very fast with Bloom filter)
    speedup1 = performance_test(
        "Test 1: Non-existent distinct_id lookup",
        "SELECT COUNT(*) FROM file('test_bloom.parquet', 'Parquet') WHERE distinct_id = '00000000-0000-0000-0000-000000000000' SETTINGS input_format_parquet_bloom_filter_push_down = 1",
        "SELECT COUNT(*) FROM file('test_no_bloom.parquet', 'Parquet') WHERE distinct_id = '00000000-0000-0000-0000-000000000000'"
    )
    
    # Test 2: Multiple non-existent distinct_ids
    speedup2 = performance_test(
        "Test 2: Multiple non-existent distinct_ids",
        "SELECT COUNT(*) FROM file('test_bloom.parquet', 'Parquet') WHERE distinct_id IN ('00000000-0000-0000-0000-000000000000', '11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222') SETTINGS input_format_parquet_bloom_filter_push_down = 1",
        "SELECT COUNT(*) FROM file('test_no_bloom.parquet', 'Parquet') WHERE distinct_id IN ('00000000-0000-0000-0000-000000000000', '11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222')"
    )
    
    # Test 3: Get a real distinct_id first, then search for it
    print("\n🔍 Getting sample distinct_ids for testing...")
    _, sample_result = run_clickhouse_query("SELECT distinct_id FROM file('test_bloom.parquet', 'Parquet') LIMIT 3")
    
    if sample_result:
        sample_id = sample_result[0][0]  # First row, first column
        print(f"   Using sample distinct_id: {sample_id[:16]}...")
        
        speedup3 = performance_test(
            "Test 3: Existing distinct_id lookup", 
            f"SELECT COUNT(*) FROM file('test_bloom.parquet', 'Parquet') WHERE distinct_id = '{sample_id}' SETTINGS input_format_parquet_bloom_filter_push_down = 1",
            f"SELECT COUNT(*) FROM file('test_no_bloom.parquet', 'Parquet') WHERE distinct_id = '{sample_id}'"
        )
    
    # Test 4: Pattern matching (should not benefit from Bloom filter)
    speedup4 = performance_test(
        "Test 4: Pattern matching (no Bloom benefit expected)",
        "SELECT COUNT(*) FROM file('test_bloom.parquet', 'Parquet') WHERE distinct_id LIKE '%-0000-%' SETTINGS input_format_parquet_bloom_filter_push_down = 1",
        "SELECT COUNT(*) FROM file('test_no_bloom.parquet', 'Parquet') WHERE distinct_id LIKE '%-0000-%'"
    )
    
    # Summary
    print(f"\n📊 SUMMARY")
    print("=" * 20)
    speedups = [s for s in [speedup1, speedup2, speedup3, speedup4] if s is not None]
    if speedups:
        avg_speedup = statistics.mean(speedups)
        print(f"Average Bloom filter speedup: {avg_speedup:.2f}x")
        
        if avg_speedup > 1.5:
            print("🎉 Bloom filters provide significant performance benefits!")
        elif avg_speedup > 1.1:
            print("👍 Bloom filters provide modest performance benefits")
        else:
            print("🤔 Bloom filters show minimal performance impact")
    else:
        print("❌ Could not calculate average speedup")

if __name__ == "__main__":
    main()