//! Configuration example

use timslite::{Config, DataType, Result, TimeStore};

fn main() -> Result<()> {
    println!("=== Timslite Configuration Example ===\n");

    // Create a custom configuration
    let config = Config::new("/tmp/timslite_config_example")
        .set_compression_level(5) // Medium compression
        .set_expiration_days(30) // Keep data for 30 days
        .enable_wal(true) // Enable write-ahead log
        .set_file_size(DataType::Wave, 128 * 1024 * 1024) // 128MB for wave files
        .set_file_size(DataType::Measure, 64 * 1024 * 1024); // 64MB for measure files

    println!("Configuration:");
    println!("  Compression level: {}", config.compression_level);
    println!("  Expiration days: {}", config.expiration_days);
    println!("  WAL enabled: {}", config.enable_wal);
    println!(
        "  Wave file size: {} MB",
        config.file_size(DataType::Wave) / 1024 / 1024
    );
    println!(
        "  Measure file size: {} MB",
        config.file_size(DataType::Measure) / 1024 / 1024
    );

    // Open store with configuration
    println!("\nOpening store with configuration...");
    let store = TimeStore::with_config(config)?;

    // Verify configuration is applied
    let store_config = store.config();
    println!("\nStore configuration:");
    println!("  Data directory: {:?}", store_config.data_dir);
    println!("  Index directory: {:?}", store_config.index_dir);
    println!("  Compression enabled: {}", store_config.enable_compression);
    println!(
        "  Flush interval: {} seconds",
        store_config.flush_interval_secs
    );
    println!("  Max idle time: {} seconds", store_config.max_idle_secs);

    // Create a dataset
    println!("\nCreating dataset...");
    let dataset = store.open_dataset("configured_dataset", DataType::Wave)?;

    // Write some data
    let timestamp = 1000i64;
    let data = vec![1u8; 1000];
    dataset.write(timestamp, &data)?;
    println!("  Written {} bytes at timestamp {}", data.len(), timestamp);

    // Flush and close
    dataset.flush()?;
    store.close()?;

    println!("\n=== Example Complete ===");
    Ok(())
}
