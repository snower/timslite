//! Manual background execution tests.
use std::fs;
use std::path::PathBuf;

fn temp_dir() -> PathBuf {
    let d = std::env::temp_dir().join("timslite_integration");
    fs::create_dir_all(&d).unwrap();
    d.join(format!(
        "test_{:?}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

use std::time::Duration;

#[test]
fn t21_1_manual_bg_lifecycle() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .flush_interval(Duration::from_millis(100))
        .idle_timeout(Duration::from_millis(500))
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "manual_bg",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("manual_bg", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();
    arc.lock().unwrap().write(1, b"bg_test").unwrap();
    drop(arc);

    // tick should execute flush (since flush interval is short)
    std::thread::sleep(Duration::from_millis(200));
    let result = store.tick_background_tasks().unwrap();
    assert!(result.executed_tasks > 0);

    // Verify data is queryable after tick
    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(1, 1, None).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1, b"bg_test");

    store.close().unwrap();
}

#[test]
fn t21_2_manual_bg_next_delay_consistency() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .flush_interval(Duration::from_secs(3600))
        .build();
    let store = Store::open(&dir, config).unwrap();

    let delay1 = store.next_background_delay().unwrap();
    let delay2 = store.next_background_delay().unwrap();
    // Two consecutive calls should return similar values
    assert!(delay1.as_secs() == delay2.as_secs());

    store.close().unwrap();
}

#[test]
fn t21_3_manual_bg_concurrent_with_thread() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(true)
        .flush_interval(Duration::from_millis(100))
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "conc_bg",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("conc_bg", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();
    arc.lock().unwrap().write(1, b"concurrent").unwrap();
    drop(arc);

    // Manual tick alongside background thread — should not deadlock
    std::thread::sleep(Duration::from_millis(200));
    let result = store.tick_background_tasks().unwrap();
    // executed_tasks may be 0 if bg thread already ran — that's fine
    assert!(result.executed_tasks <= 2);

    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(1, 1, None).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1, b"concurrent");

    store.close().unwrap();
}
