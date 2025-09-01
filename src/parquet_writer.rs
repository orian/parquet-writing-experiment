use crate::data_generator::AnalyticsData;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{WriterProperties, EnabledStatistics};
use parquet::basic::{Compression, ZstdLevel};
use parquet::schema::types::ColumnPath;
use std::fs::File;
use std::sync::Arc;

pub enum BloomFilterMode {
    Enabled,
    Disabled,
}

/// Write analytics data to Parquet file with configurable Bloom filter settings
pub fn write_parquet_file(
    data: &AnalyticsData, 
    filename: &str, 
    bloom_filter_mode: BloomFilterMode
) -> Result<(), Box<dyn std::error::Error>> {
    
    println!("Creating Arrow schema and record batch...");
    let record_batch = data.to_record_batch()?;
    let schema = AnalyticsData::get_schema();
    
    println!("Configuring Parquet writer...");
    
    // Configure writer properties based on Bloom filter mode
    let props = match bloom_filter_mode {
        BloomFilterMode::Enabled => {
            println!("  ✅ Bloom filters ENABLED for distinct_id column");
            let cp = ColumnPath::from("distinct_id");
            WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(9).unwrap()))
                .set_statistics_enabled(EnabledStatistics::Page)
                .set_bloom_filter_enabled(false)  // Disable global Bloom filters
                .set_column_bloom_filter_enabled(cp.clone(), true)
                .set_column_bloom_filter_fpp(cp, 0.1)// Enable only for distinct_id
                .build()
        }
        BloomFilterMode::Disabled => {
            println!("  ❌ Bloom filters DISABLED");
            WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(9).unwrap()))
                .set_statistics_enabled(EnabledStatistics::Page)
                .set_bloom_filter_enabled(false)  // Explicitly disable all Bloom filters
                .build()
        }
    };
    
    // Create output file
    let file = File::create(filename)?;
    
    println!("Writing Parquet file...");
    
    // Create Arrow writer with configured properties
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
    
    // Write the record batch
    writer.write(&record_batch)?;
    
    // Close writer
    writer.close()?;
    
    let bloom_status = match bloom_filter_mode {
        BloomFilterMode::Enabled => "WITH Bloom filters",
        BloomFilterMode::Disabled => "WITHOUT Bloom filters",
    };
    
    println!("✅ Created {} with {} rows ({})", 
             filename, 
             data.team_ids.len(), 
             bloom_status);
    
    Ok(())
}