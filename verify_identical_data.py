#!/usr/bin/env python3
"""
Verify that both Parquet files contain identical data
"""

import duckdb

def verify_identical_data(file1, file2):
    print(f"🧪 Verifying identical data between:")
    print(f"  📁 {file1}")  
    print(f"  📁 {file2}")
    print("=" * 60)
    
    conn = duckdb.connect()
    
    # Read both files
    print("📊 Reading both files...")
    df1 = conn.execute(f"SELECT * FROM read_parquet('{file1}') ORDER BY distinct_id").fetchdf()
    df2 = conn.execute(f"SELECT * FROM read_parquet('{file2}') ORDER BY distinct_id").fetchdf()
    
    print(f"  File 1 rows: {len(df1):,}")
    print(f"  File 2 rows: {len(df2):,}")
    
    # Compare row counts
    if len(df1) != len(df2):
        print("❌ Row counts don't match!")
        return False
    
    # Compare all data
    print("\n🔍 Comparing data...")
    
    # Check each column
    columns = ['team_id', 'timestamp', 'event', 'distinct_id', 'properties']
    all_match = True
    
    for col in columns:
        if col in df1.columns and col in df2.columns:
            matches = (df1[col] == df2[col]).all()
            print(f"  {col:12}: {'✅ MATCH' if matches else '❌ DIFFER'}")
            if not matches:
                all_match = False
                # Show first few differences
                diff_mask = df1[col] != df2[col]
                diff_count = diff_mask.sum()
                print(f"    → {diff_count} differences found")
        else:
            print(f"  {col:12}: ❌ MISSING")
            all_match = False
    
    # Sample comparison
    print(f"\n📋 Sample data comparison (first 3 rows):")
    print("File 1:")
    print(df1[['team_id', 'event', 'distinct_id']].head(3).to_string(index=False))
    print("\nFile 2:")  
    print(df2[['team_id', 'event', 'distinct_id']].head(3).to_string(index=False))
    
    # Final verdict
    print(f"\n{'='*60}")
    if all_match:
        print("✅ SUCCESS: Both files contain identical data!")
        print("🎯 Perfect for accurate Bloom filter performance comparison")
    else:
        print("❌ FAILED: Files contain different data!")
        print("⚠️  This will affect Bloom filter comparison accuracy")
    
    conn.close()
    return all_match

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python verify_identical_data.py file1.parquet file2.parquet")
        sys.exit(1)
    
    verify_identical_data(sys.argv[1], sys.argv[2])