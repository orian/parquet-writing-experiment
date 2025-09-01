#!/usr/bin/env python3
"""
ClickHouse Bloom filter test with proper settings
"""

import subprocess
import time

def run_clickhouse_query_with_settings(query, use_bloom_filter=True):
    """Run a query in ClickHouse with Bloom filter settings"""
    full_query = f"""
    SET input_format_parquet_bloom_filter_push_down = {'1' if use_bloom_filter else '0'};
    {query}
    """
    
    start_time = time.time()
    try:
        result = subprocess.run([
            'docker', 'exec', 'clickhouse-test', 'clickhouse-client', '--query', full_query
        ], capture_output=True, text=True, timeout=60)
        
        execution_time = time.time() - start_time
        
        if result.returncode == 0:
            return execution_time, result.stdout.strip()
        else:
            print(f"❌ Query failed: {result.stderr}")
            return None, None
    except subprocess.TimeoutExpired:
        print(f"❌ Query timed out after 60 seconds")
        return None, None

def test_bloom_filter_performance():
    """Test Bloom filter performance with proper settings"""
    print("🔬 Testing ClickHouse Bloom Filter Performance")
    print("=" * 50)
    
    # Test 1: Non-existent distinct_id (should benefit from Bloom filter)
    print("\n🔍 Test 1: Non-existent distinct_id lookup")
    print("-" * 40)
    
    test_query = "SELECT COUNT(*) FROM file('test_bloom.parquet', 'Parquet') WHERE distinct_id = '00000000-0000-0000-0000-000000000000'"
    
    # Run with Bloom filter enabled
    time_with_bloom, result_with = run_clickhouse_query_with_settings(test_query, use_bloom_filter=True)
    print(f"🌸 WITH Bloom filter:    {time_with_bloom:.4f}s")
    
    # Run with Bloom filter disabled  
    time_without_bloom, result_without = run_clickhouse_query_with_settings(test_query, use_bloom_filter=False)
    print(f"📄 WITHOUT Bloom filter: {time_without_bloom:.4f}s")
    
    if time_with_bloom and time_without_bloom:
        speedup = time_without_bloom / time_with_bloom
        print(f"📈 Speedup: {speedup:.2f}x")
        
        if speedup > 1.2:
            print("✅ Significant improvement with Bloom filters!")
        elif speedup > 1.05:
            print("⚠️  Modest improvement with Bloom filters")
        else:
            print("❌ No significant improvement")
    
    # Test 2: Check if Bloom filters are being used
    print("\n🔍 Test 2: Bloom filter usage verification")
    print("-" * 40)
    
    # Run EXPLAIN to see if Bloom filters are mentioned
    explain_query = "EXPLAIN indexes = 1 SELECT COUNT(*) FROM file('test_bloom.parquet', 'Parquet') WHERE distinct_id = '00000000-0000-0000-0000-000000000000'"
    
    time_explain, result_explain = run_clickhouse_query_with_settings(explain_query, use_bloom_filter=True)
    if result_explain:
        print("Query execution plan:")
        print(result_explain)
    
    # Test 3: Multiple iterations for more reliable timing
    print("\n🔍 Test 3: Multiple iterations (5 runs each)")
    print("-" * 40)
    
    with_bloom_times = []
    without_bloom_times = []
    
    for i in range(5):
        print(f"  Run {i+1}/5...")
        
        time_with, _ = run_clickhouse_query_with_settings(test_query, use_bloom_filter=True)
        if time_with:
            with_bloom_times.append(time_with)
            
        time_without, _ = run_clickhouse_query_with_settings(test_query, use_bloom_filter=False)
        if time_without:
            without_bloom_times.append(time_without)
    
    if with_bloom_times and without_bloom_times:
        avg_with = sum(with_bloom_times) / len(with_bloom_times)
        avg_without = sum(without_bloom_times) / len(without_bloom_times)
        avg_speedup = avg_without / avg_with
        
        print(f"🌸 WITH Bloom filter (avg):    {avg_with:.4f}s")
        print(f"📄 WITHOUT Bloom filter (avg): {avg_without:.4f}s") 
        print(f"📈 Average speedup: {avg_speedup:.2f}x")
        
        if avg_speedup > 1.5:
            print("🎉 Excellent Bloom filter performance!")
        elif avg_speedup > 1.2:
            print("👍 Good Bloom filter performance")
        elif avg_speedup > 1.05:
            print("⚠️  Modest Bloom filter improvement")
        else:
            print("🤔 Minimal Bloom filter impact")

def main():
    test_bloom_filter_performance()

if __name__ == "__main__":
    main()