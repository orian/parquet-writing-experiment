use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;

pub fn verify_bloom_filter(filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Verifying Bloom filters in {}...", filename);
    
    // Open the parquet file
    let file = File::open(filename)?;
    let reader = SerializedFileReader::new(file)?;
    
    // Get file metadata
    let metadata = reader.metadata();
    
    println!("Parquet file metadata:");
    println!("Version: {}", metadata.file_metadata().version());
    println!("Number of rows: {}", metadata.file_metadata().num_rows());
    println!("Number of row groups: {}", metadata.num_row_groups());
    
    // Check each row group for Bloom filters
    for i in 0..metadata.num_row_groups() {
        let row_group = metadata.row_group(i);
        println!("\nRow Group {}:", i);
        println!("  Number of columns: {}", row_group.num_columns());

        // Check each column for Bloom filters
        for j in 0..row_group.num_columns() {
            let column = row_group.column(j);
            println!("  Column {} ({}): ", j, column.column_path());

            // Check if Bloom filter exists
            if let Some(bloom_metadata) = column.bloom_filter_offset() {
                println!("    ✅ Bloom filter found at offset: {}, size: {}", bloom_metadata,
                         column.bloom_filter_length().unwrap());
                
                // Try to read the actual Bloom filter
                match reader.get_row_group(i) {
                    Ok(row_group_reader) => {
                        match row_group_reader.get_column_bloom_filter(j) {
                            Some(_bloom_filter) => {
                                println!("    ✅ Bloom filter successfully loaded");
                            }
                            None => {
                                println!("    ⚠️  Bloom filter not available");
                            }
                        }
                    }
                    Err(e) => println!("    ❌ Error accessing row group: {}", e),
                }
            } else {
                println!("    ❌ No Bloom filter found");
            }
            
            // Print other column metadata
            println!("    Compression: {:?}", column.compression());
            println!("    Encodings: {:?}", column.encodings());
            println!("    Compressed size: {} bytes", column.compressed_size());
            println!("    Uncompressed size: {} bytes", column.uncompressed_size());
        }
    }
    
    Ok(())
}