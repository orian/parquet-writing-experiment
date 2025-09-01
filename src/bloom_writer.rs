use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{WriterProperties, EnabledStatistics, BloomFilterProperties};
use parquet::basic::Compression;
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::bloom_filter::Sbbf;
use rand::prelude::*;
use std::fs::File;
use std::sync::Arc;
use uuid::Uuid;

pub fn create_parquet_with_bloom_filter() -> Result<(), Box<dyn std::error::Error>> {
    let num_rows = 1000;
    
    // Generate sample data (same as before)
    let mut rng = thread_rng();
    let now = Utc::now();
    
    let team_ids: Vec<i64> = (0..num_rows).map(|_| rng.gen_range(1..=10)).collect();
    let timestamps: Vec<i64> = (0..num_rows)
        .map(|_| {
            let minutes_ago = rng.gen_range(0..=10080);
            let timestamp = now - chrono::Duration::minutes(minutes_ago);
            timestamp.timestamp_nanos_opt().unwrap_or(0)
        })
        .collect();
    
    let events = ["page_view", "click", "signup", "login", "purchase"];
    let event_data: Vec<String> = (0..num_rows)
        .map(|_| events.choose(&mut rng).unwrap().to_string())
        .collect();
    
    let distinct_ids: Vec<String> = (0..num_rows)
        .map(|_| Uuid::new_v4().to_string())
        .collect();
    
    let browsers = ["chrome", "firefox", "safari"];
    let os_list = ["windows", "macos", "linux"];
    let screen_widths = [1920, 1366, 1440];
    
    let properties: Vec<String> = (0..num_rows)
        .map(|_| {
            let browser = browsers.choose(&mut rng).unwrap();
            let os = os_list.choose(&mut rng).unwrap();
            let screen_width = screen_widths.choose(&mut rng).unwrap();
            let bot_version = rng.gen_range(1..=5);
            
            serde_json::json!({
                "browser": browser,
                "os": os,
                "screen_width": screen_width,
                "user_agent": format!("Mozilla/5.0 (compatible; Bot/{}.0)", bot_version)
            }).to_string()
        })
        .collect();
    
    // Create Arrow schema
    let schema = Schema::new(vec![
        Field::new("team_id", DataType::Int64, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("event", DataType::Utf8, false),
        Field::new("distinct_id", DataType::Utf8, false),
        Field::new("properties", DataType::Utf8, false),
    ]);
    
    // Create Arrow arrays
    let team_id_array = Int64Array::from(team_ids);
    let timestamp_array = TimestampNanosecondArray::from(timestamps);
    let event_array = StringArray::from(event_data);
    let distinct_id_array = StringArray::from(distinct_ids);
    let properties_array = StringArray::from(properties);
    
    // Create record batch
    let record_batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(team_id_array),
            Arc::new(timestamp_array),
            Arc::new(event_array),
            Arc::new(distinct_id_array),
            Arc::new(properties_array),
        ],
    )?;
    
    // Create writer properties with Bloom filter
    let bloom_props = BloomFilterProperties::builder()
        .set_fpp(0.01)  // 1% false positive probability
        .set_ndv(num_rows as u64)  // Expected number of distinct values
        .build();
    
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_bloom_filter_properties("distinct_id".to_string(), bloom_props)
        .build();
    
    // Create output file
    let file = File::create("events_bloom_native.parquet")?;
    
    // Create Arrow writer with Bloom filter properties
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
    
    // Write the record batch
    writer.write(&record_batch)?;
    
    // Close writer
    writer.close()?;
    
    println!("✅ Created events_bloom_native.parquet with {} rows", num_rows);
    println!("Columns: team_id (int64), timestamp (timestamp), event (string), distinct_id (string), properties (string)");
    println!("✅ Bloom filter enabled for distinct_id column with 1% false positive rate");
    
    Ok(())
}