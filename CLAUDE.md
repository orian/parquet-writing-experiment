# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a multi-language project for generating and writing Parquet files with Bloom filters containing analytics event data. The project simulates PostHog-style analytics events with proper schema definition using both Python (PyArrow) and Rust (parquet-rs).

## Technology Stack

- **Python**: 3.12 (specified in .python-version) - Limited Bloom filter support
- **Rust**: 2021 edition with parquet-rs v56.1.0+ - Full Bloom filter support
- **Package Manager**: uv for Python, Cargo for Rust
- **Key Libraries**: pandas, pyarrow (Python); parquet, arrow, clap (Rust)
- **File Format**: Parquet for columnar analytics data storage

## Development Setup

**Install Dependencies:**
```bash
uv add pandas pyarrow  # Add missing dependencies to pyproject.toml
uv sync                # Install all dependencies
```

**Run Scripts:**
```bash
uv run python create_parquet.py    # Generate sample Parquet file with 1000 events
uv run python main.py              # Run basic hello world
```

## Architecture

**Key Files:**
- `create_parquet.py`: Python script that generates sample analytics event data (limited Bloom filter support)
- `main.py`: Simple Python entry point
- `src/main.rs`: Rust CLI application with command parsing (clap-based)
- `src/generator.rs`: Core Parquet generation logic with full Bloom filter support
- `src/verify_bloom.rs`: Bloom filter verification utilities
- `Cargo.toml`: Rust project dependencies and metadata
- `pyproject.toml`: Python project metadata

**Data Schema:**
The generated Parquet files contain analytics events with:
- `team_id` (int64): Team identifier
- `timestamp` (timestamp): Event occurrence time  
- `event` (string): Event type (page_view, click, signup, login, purchase)
- `distinct_id` (string): UUID for user identification
- `properties` (string): JSON-encoded event metadata

**Data Generation Pattern:**
- Creates 1000 sample rows with realistic analytics data
- Uses PyArrow schema definition for proper typing
- Generates random but realistic property combinations (browser, OS, screen width)
- Timestamps span the last week from execution time

## Current State

**Immediate Setup Required:**
The project dependencies are not declared in pyproject.toml. Before running any scripts, you must add pandas and pyarrow as dependencies using `uv add pandas pyarrow`.

**Project Status:**
- Early experimental phase
- No tests or CI/CD configured
- README.md is empty  
- Virtual environment already configured via uv

## Bloom Filter Implementation

**Python (PyArrow)**: Bloom filters are not easily accessible in the current PyArrow Python API.

**Rust (parquet-rs)**: Full Bloom filter support with parquet crate v56.1.0+
```bash
# Generate Parquet file with Bloom filters (default: 1000 rows)
cargo run -- generate

# Generate with custom filename and row count
cargo run -- generate --filename custom.parquet --rows 5000

# Verify Bloom filters in existing file
cargo run -- verify --filename events_with_bloom.parquet

# Show help
cargo run -- --help
```

**Rust Configuration:**
```rust
let props = WriterProperties::builder()
    .set_compression(Compression::SNAPPY)
    .set_statistics_enabled(EnabledStatistics::Chunk)
    .set_bloom_filter_enabled(true)
    .set_column_bloom_filter_enabled(ColumnPath::from("distinct_id"), true)
    .build();
```

## Development Notes

- Use uv commands instead of pip for dependency management  
- The project simulates PostHog analytics event structure
- Focus on efficient columnar storage and proper schema definition
- Generated Parquet files are suitable for analytics workloads
- For production Bloom filters, use Rust implementation or Apache Spark