mod bloom_test;
mod data_generator;
mod parquet_writer;
mod verify_bloom;

use bloom_test::test_bloom_filter_functionality;
use clap::{Parser, Subcommand};
use data_generator::generate_sample_data;
use parquet_writer::{write_parquet_file, BloomFilterMode};
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

    /// Test Bloom filter functionality with actual data
    Test {
        /// Input filename to test
        #[arg(short, long, default_value = "events_with_bloom.parquet")]
        filename: String,
    },

    /// Generate BOTH Parquet files (with and without Bloom filters) using identical data
    GenerateBoth {
        /// Base filename prefix (will create {prefix}_bloom.parquet and {prefix}_no_bloom.parquet)
        #[arg(short, long, default_value = "events")]
        prefix: String,

        /// Number of rows to generate (default: 1000)
        #[arg(short, long, default_value = "1000")]
        rows: usize,

        /// Seed for reproducible data generation (default: 42)
        #[arg(short, long, default_value = "42")]
        seed: u64,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Generate { filename, rows } => {
            // Generate data with a default seed for backwards compatibility
            let data = generate_sample_data(*rows, 42);
            write_parquet_file(&data, filename, BloomFilterMode::Enabled)?;
        }
        Commands::Verify { filename } => {
            verify_bloom_filter(filename)?;
        }
        Commands::Test { filename } => {
            test_bloom_filter_functionality(filename)?;
        }
        Commands::GenerateBoth { prefix, rows, seed } => {
            println!("🔄 Generating identical data for both files...");
            let data = generate_sample_data(*rows, *seed);

            let bloom_filename = format!("{}_bloom.parquet", prefix);
            let no_bloom_filename = format!("{}_no_bloom.parquet", prefix);

            println!("\n📁 Writing file WITH Bloom filters...");
            write_parquet_file(&data, &bloom_filename, BloomFilterMode::Enabled)?;

            println!("\n📁 Writing file WITHOUT Bloom filters...");
            write_parquet_file(&data, &no_bloom_filename, BloomFilterMode::Disabled)?;

            println!("\n✅ Successfully created both files:");
            println!("  🌸 {} (WITH Bloom filters)", bloom_filename);
            println!("  📄 {} (WITHOUT Bloom filters)", no_bloom_filename);
            println!("  🎯 Both files contain identical data for accurate comparison");
        }
    }

    Ok(())
}
