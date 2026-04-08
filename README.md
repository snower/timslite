# Timslite - Lightweight Time-Series Storage Library

A high-performance time-series data storage library written in Rust, inspired by MonitorCare Orbit.

## Features

- **Append-only writes**: Optimized for streaming data ingestion
- **Time-range queries**: Efficient retrieval by timestamp range
- **Memory-mapped files**: Zero-copy I/O for maximum performance
- **Data compression**: Automatic compression with configurable levels
- **Automatic expiration**: Configurable data retention policies
- **Thread-safe**: Safe concurrent access from multiple threads
- **C FFI**: Use from any programming language via C bindings

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
timslite = "0.1"
```

## Quick Start

```rust
use timslite::{TimeStore, DataType, Result};

fn main() -> Result<()> {
    // Open a time-series store
    let store = TimeStore::open("/path/to/data")?;
    
    // Open a dataset for wave data
    let dataset = store.open_dataset("monitor_001", DataType::Wave)?;
    
    // Write data
    let timestamp = 1234567890;
    let data = vec![1, 2, 3, 4, 5];
    dataset.write(timestamp, &data)?;
    
    // Read data by time range
    use timslite::types::ReadOptions;
    let options = ReadOptions {
        start_timestamp: 1234567880,
        end_timestamp: 1234567900,
        ..Default::default()
    };
    let records = dataset.read(&options)?;
    
    println!("Read {} records", records.len());
    
    // Close the store
    store.close()?;
    
    Ok(())
}
```

## API Design

### Storage Hierarchy

```
data_dir/
├── .index/                    # Global index directory
│   └── {dataset_name}/        # Per-dataset index
│       └── {timestamp}        # Index files
├── {dataset_name}/            # Dataset name (level 2)
│   ├── meta.bin              # Dataset metadata
│   ├── wave/                 # Data type (level 3)
│   │   ├── 00000000000000000000  # Data file (offset)
│   │   └── 00000000000400000000
│   ├── measure/
│   └── event/
```

### Data Types

| Type | Description | File Size | Compressed |
|------|-------------|-----------|------------|
| Index | Time index (24 bytes/sec) | 16 MB | No |
| Wave | High-frequency waveform | 64 MB | Yes |
| Measure | Measurement values | 32 MB | Yes |
| Event | Event records | 8 MB | Yes |
| ManualMeasure | Manual measurements | 8 MB | Yes |

### Opening a Store

```rust
use timslite::{TimeStore, Config};

// Simple open
let store = TimeStore::open("/data/timeseries")?;

// With configuration
let config = Config::new("/data/timeseries")
    .set_compression_level(7)
    .set_expiration_days(30)
    .enable_wal(true);

let store = TimeStore::with_config(config)?;
```

### Dataset Operations

```rust
use timslite::DataType;

// Open different dataset types
let wave_ds = store.open_dataset("patient_001", DataType::Wave)?;
let measure_ds = store.open_dataset("patient_001", DataType::Measure)?;
let event_ds = store.open_dataset("patient_001", DataType::Event)?;

// Each dataset is independent
wave_ds.write(timestamp, &wave_data)?;
measure_ds.write(timestamp, &measure_data)?;
```

### Writing Data

```rust
// Simple write
dataset.write(timestamp, &data)?;

// Write with index (for Index type)
dataset.write_with_index(timestamp, &wave_data, &measure_data)?;
```

### Reading Data

```rust
use timslite::types::ReadOptions;

let options = ReadOptions {
    start_timestamp: 1000,
    end_timestamp: 2000,
    sampling_period: 10,  // Every 10 seconds
    decompress: true,
};

let records = dataset.read(&options)?;

for record in records {
    println!("Timestamp: {}, Data length: {}", record.timestamp, record.data.len());
}
```

## C FFI Usage

```c
#include "timslite.h"

int main() {
    // Open store
    void* store = timslite_open("/data/timeseries");
    if (!store) {
        printf("Failed to open store\n");
        return -1;
    }
    
    // Open dataset
    void* dataset = timslite_open_dataset(store, "monitor_001", 1); // 1 = Wave
    if (!dataset) {
        printf("Failed to open dataset\n");
        timslite_close(store);
        return -1;
    }
    
    // Write data
    uint8_t data[] = {1, 2, 3, 4, 5};
    int64_t offset = timslite_write(dataset, 1234567890, data, 5);
    if (offset < 0) {
        printf("Write failed\n");
    }
    
    // Flush and close
    timslite_flush(dataset);
    timslite_close_dataset(dataset);
    timslite_close(store);
    
    return 0;
}
```

## Architecture

### Core Components

1. **TimeStore**: Top-level manager for all datasets
2. **Dataset**: Individual time-series collection with specific data type
3. **MappedFile**: Memory-mapped file for efficient I/O
4. **IndexManager**: Time-based index for fast lookups

### File Format

Each data file follows this structure:

```
┌─────────────────────────────────────────────┐
│ Header (38 bytes)                           │
│  - Magic: "TMSL" (4 bytes)                 │
│  - Version (4 bytes)                        │
│  - Created timestamp (8 bytes)              │
│  - Data type (4 bytes)                      │
│  - File size (8 bytes)                      │
│  - Metadata size (4 bytes)                  │
│  - Index size (4 bytes)                     │
│  - Compression type (1 byte)                │
│  - Compression level (1 byte)               │
├─────────────────────────────────────────────┤
│ State (16 bytes)                            │
│  - Write position (8 bytes)                 │
│  - Data size (8 bytes)                      │
├─────────────────────────────────────────────┤
│ Index Section (optional)                    │
│  - For self-indexed types                   │
│  - [timestamp(8) + position(8)] × N        │
├─────────────────────────────────────────────┤
│ Data Section                                │
│  - [size(4) + data(variable)] × N          │
└─────────────────────────────────────────────┘
```

## Performance

- **Write throughput**: >100K records/sec
- **Read latency**: <1ms for time-range queries
- **Compression ratio**: 50-70% for typical medical data
- **Memory overhead**: <10MB per dataset (unloaded)

## Limitations

- Append-only: No update or delete operations
- Timestamps must be roughly sequential
- Single writer per dataset (multiple readers allowed)

## Comparison with MonitorCare Orbit

| Feature | MonitorCare Orbit (Java) | Timslite (Rust) |
|---------|-------------------------|-----------------|
| Language | Java | Rust |
| Memory safety | GC | Compile-time |
| Concurrency | Synchronized | Lock-free |
| Serialization | Protobuf | Bincode/Protobuf |
| Compression | Deflate | Deflate |
| FFI | JNI | C FFI |

## Building

```bash
# Build library
cargo build --release

# Build with C FFI
cargo build --release --features ffi

# Run tests
cargo test

# Generate documentation
cargo doc --open
```

## License

MIT License

## Contributing

Contributions are welcome! Please read the contributing guidelines first.