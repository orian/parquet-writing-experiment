use human_bytes::human_bytes;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use std::env;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

fn format_bytes(size: u64) -> String {
    human_bytes(size as f64)
}

fn format_statistic(stat: &Statistics) -> String {
    let null_count = stat.null_count_opt().unwrap_or(0);
    
    if stat.min_is_exact() && stat.max_is_exact() {
        let (min_val, max_val) = match stat {
            Statistics::Boolean(s) => {
                match (s.min_bytes_opt(), s.max_bytes_opt()) {
                    (Some(min_bytes), Some(max_bytes)) => {
                        let min_val = if !min_bytes.is_empty() { min_bytes[0] != 0 } else { false };
                        let max_val = if !max_bytes.is_empty() { max_bytes[0] != 0 } else { false };
                        (min_val.to_string(), max_val.to_string())
                    }
                    _ => ("N/A".to_string(), "N/A".to_string())
                }
            }
            Statistics::Int32(s) => {
                match (s.min_bytes_opt(), s.max_bytes_opt()) {
                    (Some(min_bytes), Some(max_bytes)) => {
                        let min_val = if min_bytes.len() >= 4 {
                            i32::from_le_bytes([min_bytes[0], min_bytes[1], min_bytes[2], min_bytes[3]])
                        } else { 0 };
                        let max_val = if max_bytes.len() >= 4 {
                            i32::from_le_bytes([max_bytes[0], max_bytes[1], max_bytes[2], max_bytes[3]])
                        } else { 0 };
                        (min_val.to_string(), max_val.to_string())
                    }
                    _ => ("N/A".to_string(), "N/A".to_string())
                }
            }
            Statistics::Int64(s) => {
                match (s.min_bytes_opt(), s.max_bytes_opt()) {
                    (Some(min_bytes), Some(max_bytes)) => {
                        let min_val = if min_bytes.len() >= 8 {
                            i64::from_le_bytes([
                                min_bytes[0], min_bytes[1], min_bytes[2], min_bytes[3],
                                min_bytes[4], min_bytes[5], min_bytes[6], min_bytes[7],
                            ])
                        } else { 0 };
                        let max_val = if max_bytes.len() >= 8 {
                            i64::from_le_bytes([
                                max_bytes[0], max_bytes[1], max_bytes[2], max_bytes[3],
                                max_bytes[4], max_bytes[5], max_bytes[6], max_bytes[7],
                            ])
                        } else { 0 };
                        (min_val.to_string(), max_val.to_string())
                    }
                    _ => ("N/A".to_string(), "N/A".to_string())
                }
            }
            Statistics::Float(s) => {
                match (s.min_bytes_opt(), s.max_bytes_opt()) {
                    (Some(min_bytes), Some(max_bytes)) => {
                        let min_val = if min_bytes.len() >= 4 {
                            f32::from_le_bytes([min_bytes[0], min_bytes[1], min_bytes[2], min_bytes[3]])
                        } else { 0.0 };
                        let max_val = if max_bytes.len() >= 4 {
                            f32::from_le_bytes([max_bytes[0], max_bytes[1], max_bytes[2], max_bytes[3]])
                        } else { 0.0 };
                        (min_val.to_string(), max_val.to_string())
                    }
                    _ => ("N/A".to_string(), "N/A".to_string())
                }
            }
            Statistics::Double(s) => {
                match (s.min_bytes_opt(), s.max_bytes_opt()) {
                    (Some(min_bytes), Some(max_bytes)) => {
                        let min_val = if min_bytes.len() >= 8 {
                            f64::from_le_bytes([
                                min_bytes[0], min_bytes[1], min_bytes[2], min_bytes[3],
                                min_bytes[4], min_bytes[5], min_bytes[6], min_bytes[7],
                            ])
                        } else { 0.0 };
                        let max_val = if max_bytes.len() >= 8 {
                            f64::from_le_bytes([
                                max_bytes[0], max_bytes[1], max_bytes[2], max_bytes[3],
                                max_bytes[4], max_bytes[5], max_bytes[6], max_bytes[7],
                            ])
                        } else { 0.0 };
                        (min_val.to_string(), max_val.to_string())
                    }
                    _ => ("N/A".to_string(), "N/A".to_string())
                }
            }
            Statistics::ByteArray(s) => {
                match (s.min_bytes_opt(), s.max_bytes_opt()) {
                    (Some(min_bytes), Some(max_bytes)) => {
                        let min_str = String::from_utf8_lossy(min_bytes).to_string();
                        let max_str = String::from_utf8_lossy(max_bytes).to_string();
                        // Truncate if too long for readability
                        let min_display = if min_str.len() > 20 { 
                            format!("{}...", &min_str[..17]) 
                        } else { 
                            format!("'{}'", min_str) 
                        };
                        let max_display = if max_str.len() > 20 { 
                            format!("{}...", &max_str[..17]) 
                        } else { 
                            format!("'{}'", max_str) 
                        };
                        (min_display, max_display)
                    }
                    _ => ("N/A".to_string(), "N/A".to_string())
                }
            }
            Statistics::FixedLenByteArray(s) => {
                match (s.min_bytes_opt(), s.max_bytes_opt()) {
                    (Some(min_bytes), Some(max_bytes)) => {
                        let min_hex = min_bytes.iter().take(8).map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join("");
                        let max_hex = max_bytes.iter().take(8).map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join("");
                        let min_display = if min_bytes.len() > 8 { format!("0x{}...", min_hex) } else { format!("0x{}", min_hex) };
                        let max_display = if max_bytes.len() > 8 { format!("0x{}...", max_hex) } else { format!("0x{}", max_hex) };
                        (min_display, max_display)
                    }
                    _ => ("N/A".to_string(), "N/A".to_string())
                }
            }
            Statistics::Int96(s) => {
                match (s.min_bytes_opt(), s.max_bytes_opt()) {
                    (Some(min_bytes), Some(max_bytes)) => {
                        // Int96 is typically used for timestamps in older Parquet versions
                        let min_display = format!("Int96({:?})", &min_bytes[..std::cmp::min(12, min_bytes.len())]);
                        let max_display = format!("Int96({:?})", &max_bytes[..std::cmp::min(12, max_bytes.len())]);
                        (min_display, max_display)
                    }
                    _ => ("N/A".to_string(), "N/A".to_string())
                }
            }
        };
        
        format!(" | Stats(min: {}, max: {}, nulls: {})", min_val, max_val, null_count)
    } else if null_count > 0 {
        format!(" | Stats(nulls: {})", null_count)
    } else {
        " | Stats(no min/max)".to_string()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Usage: map_parquet <path_to_your_file.parquet>");
        return Ok(());
    }

    let file_path = &args[1];
    let path = Path::new(file_path);

    let mut file = File::open(&path)?;

    let file_size = file.metadata()?.len();

    // Manually read footer size from the last 8 bytes of the file.
    // The last 8 bytes of a Parquet file are:
    // - 4 bytes for the footer length
    // - 4 bytes for the magic number "PAR1"
    file.seek(SeekFrom::End(-8))?;
    let mut footer_len_bytes = [0; 4];
    file.read_exact(&mut footer_len_bytes)?;
    let footer_size = u32::from_le_bytes(footer_len_bytes) as u64;

    // We need to rewind the file to the beginning so that the reader can read it from the start.
    file.seek(SeekFrom::Start(0))?;

    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();

    println!(
        "Physical Layout Tree for: {} (Total Size: {})",
        path.file_name().and_then(|s| s.to_str()).unwrap_or(""),
        format_bytes(file_size)
    );
    println!("{}", "=".repeat(120));

    println!("📄 File Header");
    println!("└── Magic Number 'PAR1' @ offset 0 (4 bytes)");
    println!();

    println!("📦 Row Groups");
    for (rg_idx, row_group) in metadata.row_groups().iter().enumerate() {
        let is_last_rg = rg_idx == metadata.num_row_groups() - 1;
        let rg_prefix = if is_last_rg { "└──" } else { "├──" };

        println!(
            "{} Row Group {} ({} rows, total size: {})",
            rg_prefix,
            rg_idx,
            row_group.num_rows(),
            format_bytes(row_group.total_byte_size() as u64)
        );

        for (col_idx, column) in row_group.columns().iter().enumerate() {
            let is_last_col = col_idx == row_group.columns().len() - 1;
            let rg_cont_prefix = if is_last_rg { "    " } else { "│   " };
            let col_prefix = if is_last_col {
                "└──"
            } else {
                "├──"
            };

            let uncompressed_size_str = format_bytes(column.uncompressed_size() as u64);
            let compressed_size_str = format_bytes(column.compressed_size() as u64);

            let stats_str = if let Some(stats) = column.statistics() {
                format_statistic(stats)
            } else {
                "".to_string()
            };

            let chunk_details = format!(
                "Column '{}' ({:?}, {:?}) @ offset {} | Size: {} -> {} ({} values){}",
                column.column_path(),
                column.column_type(),
                column.compression(),
                column.file_offset(),
                uncompressed_size_str,
                compressed_size_str,
                column.num_values(),
                stats_str
            );
            println!(
                "{}{} 📊 Column Chunk: {}",
                rg_cont_prefix, col_prefix, chunk_details
            );
        }
    }
    println!();

    println!("📜 File Footer");
    let metadata_offset = file_size - footer_size - 8;
    let metadata_details = format!(
        "Version: {}, Num Rows: {}, Num RGs: {}, Created by: '{}'",
        metadata.file_metadata().version(),
        metadata.file_metadata().num_rows(),
        metadata.num_row_groups(),
        metadata.file_metadata().created_by().unwrap_or("Unknown")
    );

    println!(
        "├── Ⓜ️  File Metadata @ offset {} ({})",
        metadata_offset,
        format_bytes(footer_size)
    );
    println!("│   └── Details: {}", metadata_details);
    println!("├── 📏 Footer Length @ offset {} (4 bytes)", file_size - 8);
    println!("│   └── Value: {}", footer_size);
    println!(
        "└── Magic Number 'PAR1' @ offset {} (4 bytes)",
        file_size - 4
    );

    Ok(())
}
