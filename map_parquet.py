import sys
import os
import pyarrow.parquet as pq


def format_bytes(size):
    """Helper function to format bytes into KB, MB, etc."""
    power = 1024
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power and n < len(power_labels):
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}B"


if len(sys.argv) < 2:
    print("Usage: python map_parquet.py <path_to_your_file.parquet>")
    sys.exit(1)

file_path = sys.argv[1]

try:
    # Get total file size for context
    file_size = os.path.getsize(file_path)

    # Open the file and read metadata
    parquet_file = pq.ParquetFile(file_path)
    metadata = parquet_file.metadata

    print(f"Physical Layout Tree for: {os.path.basename(file_path)} (Total Size: {format_bytes(file_size)})")
    print("=" * 120)

    # File Header
    print("📄 File Header")
    print("└── Magic Number 'PAR1' @ offset 0 (4 bytes)")
    print()

    # Row Groups and their contents
    print("📦 Row Groups")
    for rg_idx in range(metadata.num_row_groups):
        row_group = metadata.row_group(rg_idx)
        is_last_rg = rg_idx == metadata.num_row_groups - 1
        rg_prefix = "└──" if is_last_rg else "├──"

        print(f"{rg_prefix} Row Group {rg_idx} ({row_group.num_rows} rows, total size: {format_bytes(row_group.total_byte_size)})")

        for col_idx in range(row_group.num_columns):
            column = row_group.column(col_idx)
            is_last_col = col_idx == row_group.num_columns - 1
            rg_cont_prefix = "    " if is_last_rg else "│   "
            col_prefix = "└──" if is_last_col else "├──"

            # --- Column Chunk ---
            uncompressed_size_str = format_bytes(column.total_uncompressed_size)
            compressed_size_str = format_bytes(column.total_compressed_size)

            stats_str = ""
            if column.statistics and column.statistics.has_min_max:
                stats = column.statistics
                min_val = str(stats.min)
                if len(min_val) > 15: min_val = min_val[:12] + '...'
                max_val = str(stats.max)
                if len(max_val) > 15: max_val = max_val[:12] + '...'
                stats_str = f" | Stats(min: {min_val}, max: {max_val}, nulls: {stats.null_count})"

            chunk_details = (
                f"Column '{column.path_in_schema}' ({column.physical_type}, {column.compression}) "
                f"@ offset {column.file_offset} "
                f"| Size: {uncompressed_size_str} -> {compressed_size_str} ({column.num_values} values)"
                f"{stats_str}"
            )
            print(f"{rg_cont_prefix}{col_prefix} 📊 Column Chunk: {chunk_details}")

            # --- Bloom Filter ---
            # Check for Bloom Filter by checking for the attribute's existence
            if hasattr(column, 'bloom_filter_offset'):
                col_cont_prefix = "    " if is_last_col else "│   "
                bloom_size_str = format_bytes(column.bloom_filter_length)
                bloom_details = (
                    f"For Column '{column.path_in_schema}' @ offset {column.bloom_filter_offset} "
                    f"| Size: {bloom_size_str}"
                )
                print(f"{rg_cont_prefix}{col_cont_prefix}└── 🌸 Bloom Filter: {bloom_details}")
    print()

    # The file ends with the footer and its metadata
    # The last 8 bytes are [4-byte footer length][4-byte magic number]
    print("📜 File Footer")
    with open(file_path, 'rb') as f:
        f.seek(-8, os.SEEK_END)
        footer_len_bytes = f.read(4)
        footer_len = int.from_bytes(footer_len_bytes, 'little')

    footer_offset = file_size - footer_len - 8
    metadata_details = (
        f"Version: {metadata.format_version}, "
        f"Num Rows: {metadata.num_rows}, "
        f"Num RGs: {metadata.num_row_groups}, "
        f"Created by: '{metadata.created_by}'"
    )

    print(f"├── Ⓜ️  File Metadata @ offset {footer_offset} ({format_bytes(footer_len)})")
    print(f"│   └── Details: {metadata_details}")
    print(f"├── 📏 Footer Length @ offset {file_size - 8} (4 bytes)")
    print(f"│   └── Value: {footer_len}")
    print(f"└── Magic Number 'PAR1' @ offset {file_size - 4} (4 bytes)")

except Exception as e:
    print(f"An error occurred: {e}")