import pyarrow as pa
import pyarrow.parquet as pq

def check_bloom_filter_support():
    """Check if the current PyArrow version supports Bloom filters"""
    print(f"PyArrow version: {pa.__version__}")
    
    # Check available methods
    parquet_attrs = [attr for attr in dir(pq) if 'bloom' in attr.lower()]
    print(f"Parquet module bloom-related attributes: {parquet_attrs}")
    
    # Check _parquet module
    try:
        import pyarrow._parquet as _pq
        parquet_internal_attrs = [attr for attr in dir(_pq) if 'bloom' in attr.lower() or 'writer' in attr.lower()]
        print(f"Internal _parquet module relevant attributes: {parquet_internal_attrs}")
        
        # Check WriterPropertiesBuilder methods
        if hasattr(_pq, 'WriterPropertiesBuilder'):
            builder = _pq.WriterPropertiesBuilder()
            builder_methods = [method for method in dir(builder) if 'bloom' in method.lower()]
            print(f"WriterPropertiesBuilder bloom methods: {builder_methods}")
        else:
            print("WriterPropertiesBuilder not found")
            
    except ImportError:
        print("Internal _parquet module not available")
    
    # Try to read existing parquet file metadata
    try:
        metadata = pq.read_metadata('events.parquet')
        print(f"\nParquet file metadata:")
        print(f"Number of row groups: {metadata.num_row_groups}")
        
        # Check row group metadata
        row_group = metadata.row_group(0)
        print(f"Row group 0 columns: {row_group.num_columns}")
        
        for i in range(row_group.num_columns):
            col_meta = row_group.column(i)
            print(f"Column {i} ({col_meta.path_in_schema}): {col_meta}")
            
    except FileNotFoundError:
        print("events.parquet file not found")

if __name__ == "__main__":
    check_bloom_filter_support()