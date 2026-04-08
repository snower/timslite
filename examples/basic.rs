//! Basic usage example

use timslite::types::ReadOptions;
use timslite::{DataType, Result, TimeStore};

fn main() -> Result<()> {
    // Initialize logger
    env_logger::init();

    println!("=== Timslite Basic Example ===\n");

    // Open a time-series store
    let data_dir = "/tmp/timslite_example";
    println!("Opening store at: {}", data_dir);
    let store = TimeStore::open(data_dir)?;

    // Open a dataset for wave data
    println!("\nOpening dataset 'monitor_001' with type 'wave'");
    let wave_dataset = store.open_dataset("monitor_001", DataType::Wave)?;

    // Write some data
    println!("\nWriting data...");
    let base_timestamp = 1000i64;
    for i in 0..10 {
        let timestamp = base_timestamp + i;
        let data = vec![i as u8; 100]; // 100 bytes of data
        wave_dataset.write(timestamp, &data)?;
        println!(
            "  Written timestamp: {}, data size: {} bytes",
            timestamp,
            data.len()
        );
    }

    // Flush to disk
    println!("\nFlushing to disk...");
    wave_dataset.flush()?;

    // Read data back
    println!("\nReading data...");
    let options = ReadOptions {
        start_timestamp: base_timestamp,
        end_timestamp: base_timestamp + 20,
        ..Default::default()
    };

    let records = wave_dataset.read(&options)?;
    println!("  Read {} records", records.len());

    // Open another dataset for measurement data
    println!("\nOpening dataset 'monitor_001' with type 'measure'");
    let measure_dataset = store.open_dataset("monitor_001", DataType::Measure)?;

    // Write measurement data
    println!("\nWriting measurement data...");
    for i in 0..5 {
        let timestamp = base_timestamp + i * 2;
        let data = vec![(i * 10) as u8; 50];
        measure_dataset.write(timestamp, &data)?;
        println!(
            "  Written timestamp: {}, data size: {} bytes",
            timestamp,
            data.len()
        );
    }

    // Get dataset metadata
    let meta = wave_dataset.meta();
    println!("\nDataset metadata:");
    println!("  Name: {}", meta.name);
    println!("  Type: {:?}", meta.data_type);
    println!("  Record count: {}", meta.record_count);
    println!("  Total size: {} bytes", meta.total_size);
    println!(
        "  Time range: {} - {}",
        meta.start_timestamp, meta.end_timestamp
    );

    // List all datasets
    println!("\nListing all datasets:");
    let datasets = store.list_datasets();
    for (name, dtype) in datasets {
        println!("  {} ({:?})", name, dtype);
    }

    // Close the store
    println!("\nClosing store...");
    store.close()?;

    println!("\n=== Example Complete ===");
    Ok(())
}
