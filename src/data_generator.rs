use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::compute::SortOptions;
use chrono::Utc;
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::sync::Arc;
use uuid::Uuid;

/// Sample data structure for analytics events
pub struct AnalyticsData {
    pub team_ids: Vec<i64>,
    pub timestamps: Vec<i64>,
    pub events: Vec<String>,
    pub distinct_ids: Vec<String>,
    pub properties: Vec<String>,
}

impl AnalyticsData {
    /// Sort the data by team_id, event, timestamp, distinct_id
    pub fn sort(&mut self) {
        println!("🔄 Sorting data by team_id, event, timestamp, distinct_id...");
        
        // Create a vector of indices and sort by the specified columns
        let mut indices: Vec<usize> = (0..self.team_ids.len()).collect();
        
        indices.sort_by(|&a, &b| {
            // Sort by team_id first
            self.team_ids[a]
                .cmp(&self.team_ids[b])
                // Then by event
                .then_with(|| self.events[a].cmp(&self.events[b]))
                // Then by timestamp
                .then_with(|| self.timestamps[a].cmp(&self.timestamps[b]))
                // Finally by distinct_id
                .then_with(|| self.distinct_ids[a].cmp(&self.distinct_ids[b]))
        });
        
        // Apply the sorted order to all vectors
        let sorted_team_ids = indices.iter().map(|&i| self.team_ids[i]).collect();
        let sorted_timestamps = indices.iter().map(|&i| self.timestamps[i]).collect();
        let sorted_events = indices.iter().map(|&i| self.events[i].clone()).collect();
        let sorted_distinct_ids = indices.iter().map(|&i| self.distinct_ids[i].clone()).collect();
        let sorted_properties = indices.iter().map(|&i| self.properties[i].clone()).collect();
        
        // Replace the original vectors with sorted ones
        self.team_ids = sorted_team_ids;
        self.timestamps = sorted_timestamps;
        self.events = sorted_events;
        self.distinct_ids = sorted_distinct_ids;
        self.properties = sorted_properties;
        
        println!("✅ Data sorted successfully");
    }
    
    /// Convert to Arrow RecordBatch
    pub fn to_record_batch(&self) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let schema = Self::get_schema();
        
        let team_id_array = Int64Array::from(self.team_ids.clone());
        let timestamp_array = TimestampNanosecondArray::from(self.timestamps.clone());
        let event_array = StringArray::from(self.events.clone());
        let distinct_id_array = StringArray::from(self.distinct_ids.clone());
        let properties_array = StringArray::from(self.properties.clone());
        
        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(team_id_array),
                Arc::new(timestamp_array),
                Arc::new(event_array),
                Arc::new(distinct_id_array),
                Arc::new(properties_array),
            ],
        )?;
        
        Ok(record_batch)
    }
    
    /// Get the Arrow schema
    pub fn get_schema() -> arrow::datatypes::Schema {
        arrow::datatypes::Schema::new(vec![
            Field::new("team_id", DataType::Int64, false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("event", DataType::Utf8, false),
            Field::new("distinct_id", DataType::Utf8, false),
            Field::new("properties", DataType::Utf8, false),
        ])
    }
}

/// Generate sample analytics data with deterministic seed for reproducible results
pub fn generate_sample_data(num_rows: usize, seed: u64) -> AnalyticsData {
    println!("Generating {} rows of sample data with seed {}...", num_rows, seed);
    
    // Use deterministic RNG with seed for reproducible data
    let mut rng = StdRng::seed_from_u64(seed);
    let now = Utc::now();
    
    // Generate team_ids (1-10)
    let team_ids: Vec<i64> = (0..num_rows)
        .map(|_| rng.random_range(1..=10))
        .collect();
    
    // Generate timestamps (last week)
    let timestamps: Vec<i64> = (0..num_rows)
        .map(|_| {
            let minutes_ago = rng.random_range(0..=10080); // Week in minutes
            let timestamp = now - chrono::Duration::minutes(minutes_ago);
            timestamp.timestamp_nanos_opt().unwrap_or(0)
        })
        .collect();
    
    // Generate events
    let event_types = ["page_view", "click", "signup", "login", "purchase"];
    let events: Vec<String> = (0..num_rows)
        .map(|_| event_types.choose(&mut rng).unwrap().to_string())
        .collect();
    
    // Generate distinct_ids (UUIDs) - use seed to ensure reproducibility
    let distinct_ids: Vec<String> = (0..num_rows)
        .map(|_| {
            // Generate UUID with deterministic randomness
            let mut bytes = [0u8; 16];
            rng.fill_bytes(&mut bytes);
            Uuid::from_bytes(bytes).to_string()
        })
        .collect();
    
    // Generate properties (JSON)
    let browsers = ["chrome", "firefox", "safari"];
    let os_list = ["windows", "macos", "linux"];
    let screen_widths = [1920, 1366, 1440];
    
    let properties: Vec<String> = (0..num_rows)
        .map(|_| {
            let browser = browsers.choose(&mut rng).unwrap();
            let os = os_list.choose(&mut rng).unwrap();
            let screen_width = screen_widths.choose(&mut rng).unwrap();
            let bot_version = rng.random_range(1..=5);
            
            serde_json::json!({
                "browser": browser,
                "os": os,
                "screen_width": screen_width,
                "user_agent": format!("Mozilla/5.0 (compatible; Bot/{}.0)", bot_version)
            }).to_string()
        })
        .collect();
    
    println!("✅ Generated {} rows with {} unique distinct_ids", 
             num_rows, 
             distinct_ids.iter().collect::<std::collections::HashSet<_>>().len());
    
    let mut data = AnalyticsData {
        team_ids,
        timestamps,
        events,
        distinct_ids,
        properties,
    };
    
    // Sort the data by the specified columns
    data.sort();
    
    data
}