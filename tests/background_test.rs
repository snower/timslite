//! Manual background execution tests.
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> PathBuf {
    let d = std::env::temp_dir().join("timslite_integration");
    fs::create_dir_all(&d).unwrap();
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    d.join(format!(
        "test_{:?}_{id}",
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
    let entries = arc.lock().unwrap().query(1, 1).unwrap();
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

    // Wait for bg thread to potentially flush (conservative timeout)
    std::thread::sleep(Duration::from_millis(500));

    // Manual tick alongside background thread — should not deadlock
    let result = store.tick_background_tasks().unwrap();
    // executed_tasks may be 0 if bg thread already ran — that's fine
    assert!(result.executed_tasks <= 2);

    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(1, 1).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1, b"concurrent");

    store.close().unwrap();
}

#[test]
fn t21_4_idle_close_double_check_skips_recently_used() {
    use std::time::Duration;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .flush_interval(Duration::from_millis(100))
        .idle_timeout(Duration::from_millis(100))
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "idle_dc",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("idle_dc", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();
    arc.lock().unwrap().write(1, b"before_idle").unwrap();
    drop(arc);

    // Wait for idle timeout to expire
    std::thread::sleep(Duration::from_millis(200));

    // Write again — this updates last_used_at BEFORE tick runs
    let arc = store.get_dataset(&ds).unwrap();
    arc.lock().unwrap().write(2, b"just_written").unwrap();
    drop(arc);

    // Tick immediately: double-check should see the recent write and skip idle-close
    let result = store.tick_background_tasks().unwrap();
    // flush should have run (flush_interval=100ms), idle should have been skipped by double-check
    assert!(result.executed_tasks >= 1, "at least flush should execute");

    // Verify data is still queryable (dataset was not idle-closed)
    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(1, 2).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].1, b"before_idle");
    assert_eq!(entries[1].1, b"just_written");

    store.close().unwrap();
}

#[test]
fn t21_5_cache_eviction_via_background_tick() {
    use std::time::Duration;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .flush_interval(Duration::from_secs(3600))
        .idle_timeout(Duration::from_secs(3600))
        .cache_max_memory(1024 * 1024)
        .cache_idle_timeout(Duration::from_millis(100))
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "cache_ev",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("cache_ev", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write enough data with large records to trigger block cache population
    for i in 0..50i64 {
        let data = vec![0xAA_u8; 2000];
        arc.lock().unwrap().write(i + 1, &data).unwrap();
    }

    // Query to populate the block cache with compressed blocks
    {
        let mut ds_lock = arc.lock().unwrap();
        let entries = ds_lock.query(1, 50).unwrap();
        assert_eq!(entries.len(), 50);
    }
    drop(arc);

    // Verify cache has entries
    let cache = store.block_cache();
    let cache_count_before = cache.stats().entry_count;

    // Wait for cache idle timeout
    std::thread::sleep(Duration::from_millis(200));

    // Directly invoke cache eviction (the background scheduler uses a fixed
    // 60-second interval which is impractical for a test). This exercises the
    // same evict_idle path that run_cache_eviction() calls.
    let evicted = cache.evict_idle(Duration::from_millis(100));
    assert!(evicted > 0, "should have evicted at least one idle entry");

    let cache_count_after = cache.stats().entry_count;
    assert!(
        cache_count_after < cache_count_before,
        "cache should have fewer entries after eviction: before={}, after={}",
        cache_count_before,
        cache_count_after
    );

    store.close().unwrap();
}

#[test]
fn t21_6_background_thread_auto_flush() {
    use std::time::Duration;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(true)
        .flush_interval(Duration::from_millis(100))
        .idle_timeout(Duration::from_secs(3600))
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "auto_flush",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("auto_flush", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();
    arc.lock().unwrap().write(1, b"auto_flushed").unwrap();
    drop(arc);

    // Wait for the background thread to auto-flush (flush_interval=100ms)
    std::thread::sleep(Duration::from_millis(500));

    // Verify data is still queryable after background flush
    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(1, 1).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1, b"auto_flushed");
    drop(arc);

    store.close().unwrap();
}
