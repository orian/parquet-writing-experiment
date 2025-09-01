use clap::{Parser, Subcommand};

mod generator;
mod verify_bloom;

use generator::generate_parquet_with_bloom_filter;
use verify_bloom::verify_bloom_filter;

#[derive(Parser)]
#[command(name = "parquet-bloom-writer")]
#[command(about = "A CLI tool for creating and verifying Parquet files with Bloom filters")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate a Parquet file with Bloom filters
    Generate {
        /// Output filename (default: events_with_bloom.parquet)
        #[arg(short, long, default_value = "events_with_bloom.parquet")]
        filename: String,
        
        /// Number of rows to generate (default: 1000)
        #[arg(short, long, default_value = "1000")]
        rows: usize,
    },
    
    /// Verify Bloom filters in an existing Parquet file
    Verify {
        /// Input filename to verify
        #[arg(short, long, default_value = "events_with_bloom.parquet")]
        filename: String,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    
    match &cli.command {
        Commands::Generate { filename, rows } => {
            generate_parquet_with_bloom_filter(filename, *rows)?;
        }
        Commands::Verify { filename } => {
            verify_bloom_filter(filename)?;
        }
    }
    
    Ok(())
}