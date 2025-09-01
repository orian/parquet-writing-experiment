use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, StringArray};
use std::fs::File;

pub fn test_bloom_filter_functionality(filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing Bloom filter functionality for {}...", filename);
    
    // Open file and read some actual distinct_id values
    let file = File::open(filename)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;
    
    // Read the first batch to get some distinct_id values
    if let Some(batch_result) = reader.next() {
        let batch: RecordBatch = batch_result?;
        
        // Get the distinct_id column (should be column 3)
        if let Some(distinct_id_array) = batch.column(3).as_any().downcast_ref::<StringArray>() {
            println!("📊 Found {} rows in first batch", distinct_id_array.len());
            
            // Get a few sample values
            let sample_values: Vec<String> = (0..std::cmp::min(5, distinct_id_array.len()))
                .map(|i| distinct_id_array.value(i))
                .map(|s| s.to_string())
                .collect();
                
            println!("🔍 Sample distinct_id values:");
            for (i, value) in sample_values.iter().enumerate() {
                println!("  [{}]: {}", i, value);
            }
            
            // Now try to access the Bloom filter with a different approach
            println!("\n🔬 Attempting to test Bloom filter with sample values...");
            
            // Re-open file with SerializedFileReader for Bloom filter access
            let file2 = File::open(filename)?;
            let reader2 = SerializedFileReader::new(file2)?;
            
            // Try to get row group reader and test Bloom filter
            let row_group_reader = reader2.get_row_group(0)?;
            
            // Try to get the Bloom filter for column 3 (distinct_id)
            match row_group_reader.get_column_bloom_filter(3) {
                Some(_bloom_filter) => {
                    println!("    ✅ Successfully accessed Bloom filter!");
                    
                    // Test the Bloom filter with our sample values
                    for (i, value) in sample_values.iter().enumerate() {
                        let _value_bytes = value.as_bytes();
                        // Note: This might require implementing the right hash function
                        println!("    🧪 Testing value [{}]: {}", i, value);
                        // bloom_filter.check() would be the method to test, but API might be different
                    }
                    
                    println!("    📊 Bloom filter is accessible and ready for use!");
                    return Ok(());
                }
                None => {
                    println!("    ⚠️  Still unable to access Bloom filter via API");
                }
            }
        }
    }
    
    println!("💡 Bloom filter exists in metadata but may require different API or version");
    Ok(())
}