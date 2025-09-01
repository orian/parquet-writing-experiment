import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import uuid
import json
import random

def create_sample_parquet():
    # Sample data
    num_rows = 1000
    
    data = {
        'team_id': [random.randint(1, 10) for _ in range(num_rows)],
        'timestamp': [datetime.now() - timedelta(minutes=random.randint(0, 10080)) for _ in range(num_rows)],
        'event': [random.choice(['page_view', 'click', 'signup', 'login', 'purchase']) for _ in range(num_rows)],
        'distinct_id': [str(uuid.uuid4()) for _ in range(num_rows)],
        'properties': [json.dumps({
            'browser': random.choice(['chrome', 'firefox', 'safari']),
            'os': random.choice(['windows', 'macos', 'linux']),
            'screen_width': random.choice([1920, 1366, 1440]),
            'user_agent': f'Mozilla/5.0 (compatible; Bot/{random.randint(1, 5)}.0)'
        }) for _ in range(num_rows)]
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Define schema with proper types
    schema = pa.schema([
        ('team_id', pa.int64()),
        ('timestamp', pa.timestamp('ns')),
        ('event', pa.string()),
        ('distinct_id', pa.string()),
        ('properties', pa.string())
    ])
    
    # Convert to PyArrow table with explicit schema
    table = pa.Table.from_pandas(df, schema=schema)
    
    # Write to Parquet file with Bloom filter
    # Create writer properties with Bloom filter
    try:
        # Attempt to use the internal _parquet module for Bloom filters
        import pyarrow._parquet as _pq
        
        # Build writer properties with Bloom filter
        builder = _pq.WriterPropertiesBuilder()
        builder.enable_bloom_filter('distinct_id')
        properties = builder.build()
        
        # Write using ParquetWriter with custom properties
        writer = pq.ParquetWriter('events.parquet', table.schema, writer_properties=properties)
        writer.write_table(table)
        writer.close()
        
        bloom_enabled = True
    except (ImportError, AttributeError):
        # Fallback to regular write if Bloom filter API is not available
        pq.write_table(table, 'events.parquet', write_statistics=True)
        bloom_enabled = False
    print(f"Created events.parquet with {num_rows} rows")
    print("Columns: team_id (int), timestamp (datetime), event (string), distinct_id (uuid), properties (JSON)")
    print(f"Bloom filter enabled for distinct_id column: {bloom_enabled}")

if __name__ == "__main__":
    create_sample_parquet()