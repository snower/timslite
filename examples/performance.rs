//! High-performance write example

use std::time::Instant;
use timslite::{DataType, Result, TimeStore};

fn main() -> Result<()> {
    println!("=== Timslite Performance Example ===\n");

    let store = TimeStore::open("/tmp/timslite_perf_example")?;
    let dataset = store.open_dataset("perf_test", DataType::Wave)?;

    // Configuration
    let num_records = 10_000;
    let data_size = 1000; // bytes per record
    let batch_size = 100;

    println!("Writing {} records ({} bytes each)", num_records, data_size);
    println!("Batch size: {} records", batch_size);

    let start = Instant::now();
    let mut written = 0u64;

    for i in 0..num_records {
        let timestamp = 1000i64 + i as i64;
        let data = vec![(i % 256) as u8; data_size];

        dataset.write(timestamp, &data)?;
        written += data_size as u64;

        if (i + 1) % batch_size == 0 {
            print!("\r  Written {}/{} records", i + 1, num_records);
        }
    }

    let duration = start.elapsed();
    println!("\n\nWrite completed in {:?}", duration);

    // Calculate throughput
    let throughput = written as f64 / duration.as_secs_f64() / 1024.0 / 1024.0;
    println!("Throughput: {:.2} MB/s", throughput);
    println!(
        "Records/sec: {:.2}",
        num_records as f64 / duration.as_secs_f64()
    );

    // Flush
    println!("\nFlushing to disk...");
    let flush_start = Instant::now();
    dataset.flush()?;
    println!("Flush completed in {:?}", flush_start.elapsed());

    // Get metadata
    let meta = dataset.meta();
    println!("\nDataset statistics:");
    println!("  Total records: {}", meta.record_count);
    println!("  Total size: {} MB", meta.total_size / 1024 / 1024);
    println!(
        "  Time range: {} - {}",
        meta.start_timestamp, meta.end_timestamp
    );

    // Cleanup
    store.close()?;

    println!("\n=== Example Complete ===");
    Ok(())
}
