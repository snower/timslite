//! Queue integration tests: push/poll/ack, consumer groups, persistence, threading.
use std::fs;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};

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
fn t27_1_1_basic_push_poll_ack() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("g1").unwrap();

    let ts = q.push(b"hello").unwrap();
    assert!(ts > 0);

    let (rts, data) = c.poll(Duration::from_millis(100)).unwrap().unwrap();
    assert_eq!(rts, ts);
    assert_eq!(data, b"hello");

    c.ack(rts).unwrap();
    assert!(c.poll(Duration::from_millis(50)).unwrap().is_none());
    store.close().unwrap();
}

#[test]
fn t27_1_2_multiple_pushes_sequential_poll() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("g1").unwrap();

    for i in 0..10i64 {
        let ts = q.push(&format!("msg_{}", i).into_bytes()).unwrap();
        assert_eq!(ts, i + 1);
    }

    for i in 0..10i64 {
        let (ts, data) = c.poll(Duration::from_millis(50)).unwrap().unwrap();
        assert_eq!(ts, i + 1);
        assert_eq!(data, format!("msg_{}", i).as_bytes());
        c.ack(ts).unwrap();
    }

    assert!(c.poll(Duration::from_millis(50)).unwrap().is_none());
    store.close().unwrap();
}

#[test]
fn t27_1_3_poll_timeout_empty_queue() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("g1").unwrap();

    let result = c.poll(Duration::from_millis(50)).unwrap();
    assert!(result.is_none());
    store.close().unwrap();
}

#[test]
fn t27_1_4_poll_skips_continuous_filler_gap() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let h = store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 1, 0)
        .unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("g1").unwrap();

    // Write 3 records
    h.write(10, b"first").unwrap();
    h.write(20, b"second").unwrap();
    h.write(30, b"third").unwrap();

    // Delete the middle record (creates a filler/gap)
    h.delete(20).unwrap();

    // Poll should skip deleted ts=20 and return ts=10 first
    let (ts, data) = c.poll(Duration::from_millis(100)).unwrap().unwrap();
    assert_eq!(ts, 10);
    assert_eq!(data, b"first");
    c.ack(ts).unwrap();

    // Poll should skip ts=20 (deleted/filler) and return ts=30
    let (ts, data) = c.poll(Duration::from_millis(100)).unwrap().unwrap();
    assert_eq!(ts, 30);
    assert_eq!(data, b"third");
    c.ack(ts).unwrap();

    assert!(c.poll(Duration::from_millis(50)).unwrap().is_none());
    store.close().unwrap();
}

#[test]
fn t27_1_6_poll_skips_natural_gap_filler() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let h = store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 1, 0)
        .unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("g1").unwrap();

    h.write(10, b"first").unwrap();
    h.write(30, b"third").unwrap();

    let (ts, data) = c.poll(Duration::from_millis(100)).unwrap().unwrap();
    assert_eq!(ts, 10);
    assert_eq!(data, b"first");
    c.ack(ts).unwrap();

    let (ts, data) = c.poll(Duration::from_millis(100)).unwrap().unwrap();
    assert_eq!(ts, 30);
    assert_eq!(data, b"third");
    c.ack(ts).unwrap();

    assert!(c.poll(Duration::from_millis(50)).unwrap().is_none());

    store.close().unwrap();

    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();

    assert!(!h.read_exist(20).unwrap(), "ts=20 gap should not exist");
    assert!(h.read_exist(10).unwrap(), "ts=10 should exist");
    assert!(h.read_exist(30).unwrap(), "ts=30 should exist");

    let exist_map = h.query_exist(1, 40).unwrap();
    assert_eq!(exist_map.len(), 5, "40 timestamps = 5 bytes bitmap");
    assert_ne!(exist_map[1] & (1u8 << 1), 0, "ts=10 should exist in bitmap");
    assert_eq!(
        exist_map[2] & (1u8 << 3),
        0,
        "ts=20 gap should not exist in bitmap"
    );
    assert_ne!(exist_map[3] & (1u8 << 5), 0, "ts=30 should exist in bitmap");
}

#[test]
fn t27_1_5_sparse_gap_acked_progress_persists_after_reopen() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    {
        let mut store = Store::open(
            &dir,
            StoreConfig::builder()
                .enable_background_thread(false)
                .build(),
        )
        .unwrap();
        let h = store
            .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 1, 0)
            .unwrap();
        let q = h.open_queue().unwrap();
        let c = q.open_consumer("g1").unwrap();

        h.write(10, b"first").unwrap();
        h.write(20, b"second").unwrap();
        h.write(30, b"third").unwrap();
        h.delete(20).unwrap();

        let (ts, data) = c.poll(Duration::from_millis(100)).unwrap().unwrap();
        assert_eq!(ts, 10);
        assert_eq!(data, b"first");
        c.ack(ts).unwrap();

        let (ts, data) = c.poll(Duration::from_millis(100)).unwrap().unwrap();
        assert_eq!(ts, 30);
        assert_eq!(data, b"third");
        c.ack(ts).unwrap();

        store.close().unwrap();
    }
    {
        let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
        let h = store.open_dataset("t27q", "events").unwrap();
        let q = h.open_queue().unwrap();
        let c = q.open_consumer("g1").unwrap();

        assert!(c.poll(Duration::from_millis(50)).unwrap().is_none());
        store.close().unwrap();
    }
}

#[test]
fn t27_2_1_multi_consumer_groups() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let ca = q.open_consumer("ga").unwrap();
    let cb = q.open_consumer("gb").unwrap();

    q.push(b"item1").unwrap();
    q.push(b"item2").unwrap();

    let (ts_a, data_a) = ca.poll(Duration::from_millis(50)).unwrap().unwrap();
    assert_eq!(data_a, b"item1");
    ca.ack(ts_a).unwrap();

    let (ts_b, data_b) = cb.poll(Duration::from_millis(50)).unwrap().unwrap();
    assert_eq!(data_b, b"item1");
    cb.ack(ts_b).unwrap();

    let (_, data_a2) = ca.poll(Duration::from_millis(50)).unwrap().unwrap();
    assert_eq!(data_a2, b"item2");

    let (_, data_b2) = cb.poll(Duration::from_millis(50)).unwrap().unwrap();
    assert_eq!(data_b2, b"item2");

    store.close().unwrap();
}

#[test]
fn t27_2_2_two_consumers_same_group() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let c1 = q.open_consumer("shared").unwrap();
    let c2 = q.open_consumer("shared").unwrap();

    q.push(b"shared_item").unwrap();

    let (ts, _) = c1.poll(Duration::from_millis(50)).unwrap().unwrap();
    assert_eq!(ts, 1);
    assert!(c2.poll(Duration::from_millis(10)).unwrap().is_none());

    store.close().unwrap();
}

#[test]
fn t41_1_same_group_unexpired_pending_does_not_block_next_record() {
    use timslite::{QueueConsumerConfig, Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t41q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t41q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let config = QueueConsumerConfig::builder()
        .running_expired_seconds(60)
        .max_retry_count(3)
        .build()
        .unwrap();
    let c1 = q.open_consumer_with_config("shared", config).unwrap();
    let c2 = q.open_consumer_with_config("shared", config).unwrap();

    q.push(b"first").unwrap();
    q.push(b"second").unwrap();

    let (ts1, data1) = c1.poll(Duration::from_millis(50)).unwrap().unwrap();
    assert_eq!(ts1, 1);
    assert_eq!(data1, b"first");

    let (ts2, data2) = c2.poll(Duration::from_millis(50)).unwrap().unwrap();
    assert_eq!(ts2, 2);
    assert_eq!(data2, b"second");

    c1.ack(ts1).unwrap();
    c2.ack(ts2).unwrap();
    store.close().unwrap();
}

#[test]
fn t41_2_expired_pending_retries_once_then_drops_and_advances() {
    use timslite::{QueueConsumerConfig, Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t41q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t41q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let config = QueueConsumerConfig::builder()
        .running_expired_seconds(1)
        .max_retry_count(1)
        .build()
        .unwrap();
    let c = q.open_consumer_with_config("retry", config).unwrap();

    q.push(b"first").unwrap();
    q.push(b"second").unwrap();

    let (ts1, data1) = c.poll(Duration::from_millis(50)).unwrap().unwrap();
    assert_eq!(ts1, 1);
    assert_eq!(data1, b"first");

    std::thread::sleep(Duration::from_millis(1100));
    let (retry_ts, retry_data) = c.poll(Duration::from_millis(50)).unwrap().unwrap();
    assert_eq!(retry_ts, 1);
    assert_eq!(retry_data, b"first");

    std::thread::sleep(Duration::from_millis(1100));
    let (next_ts, next_data) = c.poll(Duration::from_millis(50)).unwrap().unwrap();
    assert_eq!(next_ts, 2);
    assert_eq!(next_data, b"second");

    c.ack(next_ts).unwrap();
    store.close().unwrap();
}

#[test]
fn t41_3_same_group_rejects_config_mismatch() {
    use timslite::{QueueConsumerConfig, Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t41q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t41q", "events").unwrap();
    let q = h.open_queue().unwrap();

    q.open_consumer("shared").unwrap();
    let mismatched = QueueConsumerConfig::builder()
        .running_expired_seconds(30)
        .max_retry_count(3)
        .build()
        .unwrap();
    assert!(q.open_consumer_with_config("shared", mismatched).is_err());

    store.close().unwrap();
}

#[test]
fn t27_2_3_open_consumer_creates_group() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("new_group").unwrap();
    q.push(b"test").unwrap();
    let result = c.poll(Duration::from_millis(100)).unwrap();
    assert!(result.is_some());
    store.close().unwrap();
}

#[test]
fn t27_2_4_consumer_group_name_must_be_path_safe() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();

    for group_name in [
        "",
        ".",
        "..",
        "../bad",
        "bad/name",
        "bad\\name",
        "bad name",
        "bad.name",
    ] {
        assert!(
            q.open_consumer(group_name).is_err(),
            "group name {group_name:?} must be rejected"
        );
    }
    let too_long_group = "a".repeat(256);
    assert!(
        q.open_consumer(&too_long_group).is_err(),
        "group name longer than 255 bytes must be rejected"
    );

    q.open_consumer("A-z_09").unwrap();
    store.close().unwrap();
}

#[test]
fn t27_3_1_open_queue_twice_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    h.open_queue().unwrap();
    assert!(h.open_queue().is_err());
    store.close().unwrap();
}

#[test]
fn t27_3_2_push_to_closed_queue_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    h.close_queue().unwrap();
    assert!(q.push(b"test").is_err());
    store.close().unwrap();
}

#[test]
fn t27_3_3_poll_after_close_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("g1").unwrap();
    h.close_queue().unwrap();
    assert!(c.poll(Duration::from_millis(50)).is_err());
    store.close().unwrap();
}

#[test]
fn t27_3_4_ack_nonexistent_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("g1").unwrap();
    assert!(c.ack(99999).is_err());
    store.close().unwrap();
}

#[test]
fn t27_3_5_drop_nonexistent_consumer_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    assert!(q.drop_consumer("no_such").is_err());
    store.close().unwrap();
}

#[test]
fn t27_3_5_open_consumer_on_closed_queue_errors() {
    use timslite::{Store, StoreConfig, TmslError};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    h.close_queue().unwrap();
    let err = q.open_consumer("g1").unwrap_err();
    assert!(matches!(err, TmslError::QueueClosed(_)));
    store.close().unwrap();
}

#[test]
fn t27_3_6_drop_nonexistent_consumer_group_errors() {
    use timslite::{Store, StoreConfig, TmslError};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let err = q.drop_consumer("no_such_group").unwrap_err();
    assert!(matches!(err, TmslError::ConsumerGroupNotFound(_)));
    store.close().unwrap();
}

#[test]
fn t27_4_1_pending_survives_reopen() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    {
        let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
        store
            .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
            .unwrap();
        let h = store.open_dataset("t27q", "events").unwrap();
        let q = h.open_queue().unwrap();
        let c = q.open_consumer("g1").unwrap();

        q.push(b"a").unwrap();
        q.push(b"b").unwrap();
        q.push(b"c").unwrap();

        let (ts1, _) = c.poll(Duration::from_millis(50)).unwrap().unwrap();
        c.ack(ts1).unwrap();

        let (ts2, _) = c.poll(Duration::from_millis(50)).unwrap().unwrap();
        assert_eq!(ts2, 2);
        store.close().unwrap();
    }
    {
        let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
        let h = store.open_dataset("t27q", "events").unwrap();
        let q = h.open_queue().unwrap();
        let c = q.open_consumer("g1").unwrap();

        let (ts, data) = c.poll(Duration::from_millis(50)).unwrap().unwrap();
        assert_eq!(ts, 2);
        assert_eq!(data, b"b");
        c.ack(ts).unwrap();

        let (ts3, data3) = c.poll(Duration::from_millis(50)).unwrap().unwrap();
        assert_eq!(ts3, 3);
        assert_eq!(data3, b"c");

        store.close().unwrap();
    }
}

#[test]
fn t27_4_2_drop_and_recreate_consumer() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    q.open_consumer("temp").unwrap();
    q.drop_consumer("temp").unwrap();
    q.open_consumer("temp").unwrap();
    store.close().unwrap();
}

#[test]
fn t27_4_3_acked_progress_persists() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("g1").unwrap();
    for i in 0..3 {
        let ts = q.push(format!("item{}", i).as_bytes()).unwrap();
        c.poll(Duration::from_millis(100)).unwrap().unwrap();
        c.ack(ts).unwrap();
    }
    store.close().unwrap();
    let mut store2 = Store::open(&dir, StoreConfig::default()).unwrap();
    let h2 = store2.open_dataset("t27q", "events").unwrap();
    let q2 = h2.open_queue().unwrap();
    let c2 = q2.open_consumer("g1").unwrap();
    assert!(c2.poll(Duration::from_millis(50)).unwrap().is_none());
    store2.close().unwrap();
}

#[test]
fn t27_5_1_producer_consumer_threads() {
    use std::sync::{Arc, Barrier};
    use std::thread;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = Arc::new(h.open_queue().unwrap());
    let q_prod = q.clone();
    let q_cons = q.clone();
    let barrier = Arc::new(Barrier::new(2));
    let b_prod = barrier.clone();
    let b_cons = barrier.clone();

    let producer = thread::spawn(move || {
        b_prod.wait();
        for i in 0..10i64 {
            q_prod.push(format!("p_{}", i).as_bytes()).unwrap();
            thread::sleep(Duration::from_millis(1));
        }
    });

    let consumer = thread::spawn(move || {
        let c = q_cons.open_consumer("workers").unwrap();
        b_cons.wait();

        let mut count = 0;
        for _ in 0..10 {
            if let Some((ts, _)) = c.poll(Duration::from_secs(5)).unwrap() {
                c.ack(ts).unwrap();
                count += 1;
            }
        }
        assert_eq!(count, 10);
    });

    producer.join().unwrap();
    consumer.join().unwrap();
    store.close().unwrap();
}

#[test]
fn t27_5_2_multiple_producers() {
    use std::sync::{Arc, Barrier};
    use std::thread;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = Arc::new(h.open_queue().unwrap());
    let qc = q.open_consumer("g1").unwrap();
    let q1 = q.clone();
    let q2 = q.clone();
    let q3 = q.clone();
    let cc = qc.clone();
    let barrier = Arc::new(Barrier::new(4));
    let mut prod_handles = Vec::new();
    for (i, q_ref) in [q1, q2, q3].into_iter().enumerate() {
        let b = Arc::clone(&barrier);
        prod_handles.push(thread::spawn(move || {
            b.wait();
            for j in 0..7 {
                q_ref.push(format!("p{}_{}", i, j).as_bytes()).unwrap();
            }
        }));
    }
    let consumer = thread::spawn(move || {
        barrier.wait();
        thread::sleep(Duration::from_millis(20));
        let mut count = 0;
        for _ in 0..21 {
            match cc.poll(Duration::from_secs(10)) {
                Ok(Some((ts, _data))) => {
                    cc.ack(ts).unwrap();
                    count += 1;
                }
                Ok(None) => panic!("timeout"),
                Err(e) => panic!("err: {:?}", e),
            }
        }
        assert_eq!(count, 21);
    });
    for ph in prod_handles {
        ph.join().unwrap();
    }
    consumer.join().unwrap();
    store.close().unwrap();
}

#[test]
fn t27_5_3_multiple_consumers_different_groups() {
    use std::sync::{Arc, Barrier};
    use std::thread;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let ca = q.open_consumer("group_a").unwrap();
    let cb = q.open_consumer("group_b").unwrap();
    for i in 0..5 {
        q.push(format!("msg_{}", i).as_bytes()).unwrap();
    }
    let ca_clone = ca.clone();
    let cb_clone = cb.clone();
    let barrier = Arc::new(Barrier::new(2));
    let b1 = Arc::clone(&barrier);
    let b2 = Arc::clone(&barrier);
    let t1 = thread::spawn(move || {
        b1.wait();
        let mut count = 0;
        for _ in 0..5 {
            let (ts, _) = ca_clone.poll(Duration::from_secs(5)).unwrap().unwrap();
            ca_clone.ack(ts).unwrap();
            count += 1;
        }
        count
    });
    let t2 = thread::spawn(move || {
        b2.wait();
        let mut count = 0;
        for _ in 0..5 {
            let (ts, _) = cb_clone.poll(Duration::from_secs(5)).unwrap().unwrap();
            cb_clone.ack(ts).unwrap();
            count += 1;
        }
        count
    });
    assert_eq!(t1.join().unwrap(), 5);
    assert_eq!(t2.join().unwrap(), 5);
    store.close().unwrap();
}

#[test]
fn t27_6_1_closed_dataset_open_queue_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let dataset = store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    dataset.close().unwrap();
    assert!(dataset.open_queue().is_err());
    store.close().unwrap();
}

#[test]
fn t27_6_2_store_close_queue() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    q.push(b"x").unwrap();
    h.close_queue().unwrap();
    assert!(q.push(b"y").is_err());
    store.close().unwrap();
}

#[test]
fn t27_6_3_direct_queue_close_releases_dataset_queue_state() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    q.close().unwrap();

    assert!(q.push(b"old").is_err());
    let q2 = h.open_queue().unwrap();
    assert!(q2.push(b"new").is_ok());
    store.close().unwrap();
}

#[test]
fn t27_6_1_store_open_queue_valid() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("g1").unwrap();
    q.push(b"data").unwrap();
    assert!(c.poll(Duration::from_millis(100)).unwrap().is_some());
    store.close().unwrap();
}

#[test]
fn t27_6_4_store_open_consumer_and_drop_consumer() {
    use timslite::{Store, StoreConfig, TmslError};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("g1").unwrap();
    q.push(b"test").unwrap();
    assert!(c.poll(Duration::from_millis(100)).unwrap().is_some());
    q.drop_consumer("g1").unwrap();
    let err = q.drop_consumer("g1").unwrap_err();
    assert!(matches!(err, TmslError::ConsumerGroupNotFound(_)));
    store.close().unwrap();
}

#[test]
fn t27_7_push_notification_wakes_blocking_consumer_poll() {
    // End-to-end: consumer is blocked in poll() when push() fires.
    // The Condvar notification in notify_queue() should wake the consumer
    // well before the poll timeout expires.
    use std::thread;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("g1").unwrap();

    // Push from a background thread after a short delay
    let q_clone = q.clone();
    let producer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(300));
        q_clone.push(b"wake_up_data").unwrap();
    });

    // Poll with a 5-second timeout; should return quickly via Condvar wake
    let start = std::time::Instant::now();
    let result = c.poll(Duration::from_secs(5)).unwrap();
    let elapsed = start.elapsed();

    assert!(
        result.is_some(),
        "poll should return data after push notification"
    );
    let (ts, data) = result.unwrap();
    assert_eq!(data, b"wake_up_data");
    assert!(ts > 0);
    // Condvar wake should be well under the 5s timeout
    assert!(
        elapsed < Duration::from_secs(3),
        "poll took {:?}, expected < 3s (Condvar wake-up, not timeout)",
        elapsed
    );

    producer.join().unwrap();
    store.close().unwrap();
}

#[test]
fn t44_1_poll_callback_runs_for_dataset_queue_write_and_can_be_cleared() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let h = store
        .create_dataset(
            "t44callback",
            "events",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("g1").unwrap();
    let c2 = q.open_consumer("g2").unwrap();

    let hits = Arc::new(AtomicUsize::new(0));
    let callback_hits = Arc::clone(&hits);
    c.poll_callback(Some(Arc::new(move || {
        callback_hits.fetch_add(1, Ordering::SeqCst);
    })))
    .unwrap();
    let duplicate_hits = Arc::new(AtomicUsize::new(0));
    let callback_duplicate_hits = Arc::clone(&duplicate_hits);
    assert!(c
        .poll_callback(Some(Arc::new(move || {
            callback_duplicate_hits.fetch_add(1, Ordering::SeqCst);
        })))
        .is_err());

    let c2_hits = Arc::new(AtomicUsize::new(0));
    let callback_c2_hits = Arc::clone(&c2_hits);
    c2.poll_callback(Some(Arc::new(move || {
        callback_c2_hits.fetch_add(1, Ordering::SeqCst);
    })))
    .unwrap();

    h.write(1, b"row1").unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(duplicate_hits.load(Ordering::SeqCst), 0);
    assert_eq!(c2_hits.load(Ordering::SeqCst), 1);

    let (ts, data) = c.poll(Duration::from_millis(0)).unwrap().unwrap();
    assert_eq!(ts, 1);
    assert_eq!(data, b"row1");
    c.ack(ts).unwrap();
    let (ts, data) = c2.poll(Duration::from_millis(0)).unwrap().unwrap();
    assert_eq!(ts, 1);
    assert_eq!(data, b"row1");
    c2.ack(ts).unwrap();

    c.poll_callback(None).unwrap();
    h.write(2, b"row2").unwrap();
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "clearing the callback must stop future wake callbacks"
    );
    assert_eq!(c2_hits.load(Ordering::SeqCst), 2);

    let (ts, data) = c.poll(Duration::from_millis(0)).unwrap().unwrap();
    assert_eq!(ts, 2);
    assert_eq!(data, b"row2");
    c.ack(ts).unwrap();
    let (ts, data) = c2.poll(Duration::from_millis(0)).unwrap().unwrap();
    assert_eq!(ts, 2);
    assert_eq!(data, b"row2");
    c2.ack(ts).unwrap();
    store.close().unwrap();
}

// 閳光偓閳光偓閳光偓 Queue boundary tests (P0-Q-1~7) 閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓

#[test]
fn t27_7_1_push_to_closed_queue_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();

    // Push works before close
    q.push(b"before_close").unwrap();

    // Close the queue
    h.close_queue().unwrap();

    // Push after close should fail
    let result = q.push(b"after_close");
    assert!(result.is_err(), "push to closed queue should return error");

    store.close().unwrap();
}

#[test]
fn t27_7_2_poll_closed_consumer_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("g1").unwrap();

    // Push some data first
    q.push(b"data").unwrap();

    // Close the queue
    h.close_queue().unwrap();

    // Poll after close should fail
    let result = c.poll(Duration::from_millis(50));
    assert!(result.is_err(), "poll on closed queue should return error");

    store.close().unwrap();
}

#[test]
fn t27_7_3_ack_closed_consumer_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("g1").unwrap();

    // Push and poll to get a timestamp
    let ts = q.push(b"data").unwrap();
    let polled = c.poll(Duration::from_millis(50)).unwrap().unwrap();
    assert_eq!(polled.0, ts);

    // Close the queue
    h.close_queue().unwrap();

    // Ack after close should fail
    let result = c.ack(ts);
    assert!(result.is_err(), "ack on closed queue should return error");

    store.close().unwrap();
}

#[test]
fn t27_7_4_drop_consumer_twice_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();

    // Open a consumer group
    q.open_consumer("g1").unwrap();

    // First drop should succeed
    q.drop_consumer("g1").unwrap();

    // Second drop should fail
    let result = q.drop_consumer("g1");
    assert!(result.is_err(), "drop consumer twice should return error");

    store.close().unwrap();
}

#[test]
fn t27_7_5_poll_timeout_precision() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("g1").unwrap();

    // Poll with 200ms timeout on empty queue
    let start = std::time::Instant::now();
    let result = c.poll(Duration::from_millis(200)).unwrap();
    let elapsed = start.elapsed();

    assert!(result.is_none(), "empty queue should return None");

    // Timeout should be within reasonable range (200ms 鍗?100ms)
    assert!(
        elapsed >= Duration::from_millis(150) && elapsed < Duration::from_millis(500),
        "timeout precision: expected ~200ms, got {:?}",
        elapsed
    );

    store.close().unwrap();
}

#[test]
fn t27_7_6_push_empty_data() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("g1").unwrap();

    // Push empty data - should succeed (empty message is valid)
    let ts = q.push(b"").unwrap();
    assert!(ts > 0);

    // Poll should return the empty data
    let (rts, data) = c.poll(Duration::from_millis(50)).unwrap().unwrap();
    assert_eq!(rts, ts);
    assert!(
        data.is_empty(),
        "empty push should result in empty poll data"
    );

    store.close().unwrap();
}

#[test]
fn t27_7_7_consumer_group_name_boundary() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = h.open_queue().unwrap();

    // Valid: long name (within PATH_COMPONENT_MAX_LEN = 255)
    let long_name = "a".repeat(200);
    q.open_consumer(&long_name).unwrap();

    // Valid: name with hyphens and underscores
    q.open_consumer("my-group_1").unwrap();

    // Valid: numeric name
    q.open_consumer("12345").unwrap();

    // Invalid: empty name
    let result = q.open_consumer("");
    assert!(result.is_err(), "empty consumer group name should fail");

    // Invalid: name with spaces
    let result = q.open_consumer("has space");
    assert!(
        result.is_err(),
        "consumer group name with space should fail"
    );

    // Invalid: name with special characters
    let result = q.open_consumer("group/name");
    assert!(
        result.is_err(),
        "consumer group name with slash should fail"
    );

    store.close().unwrap();
}

#[test]
fn t45_1_get_consumer_group_names_lists_state_file_entries_without_opening() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let h = store
        .create_dataset(
            "t45groups",
            "events",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();
    let q = h.open_queue().unwrap();
    q.open_consumer("group_a").unwrap();
    q.open_consumer("group_b").unwrap();
    fs::write(
        h.base_dir().join("queue").join("not_a_qstf"),
        b"not a state file",
    )
    .unwrap();
    q.close().unwrap();

    let q2 = h.open_queue().unwrap();
    let names = q2.get_consumer_group_names().unwrap();
    assert_eq!(names, vec!["group_a", "group_b", "not_a_qstf"]);
    store.close().unwrap();
}

#[test]
fn t45_2_consumer_inspect_and_flush_return_state_snapshot() {
    use timslite::{QueueConsumerConfig, Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();
    let h = store
        .create_dataset(
            "t45inspect",
            "events",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();
    let q = h.open_queue().unwrap();
    let consumer_config = QueueConsumerConfig::builder()
        .running_expired_seconds(7)
        .max_retry_count(2)
        .build()
        .unwrap();
    let c = q
        .open_consumer_with_config("inspectors", consumer_config)
        .unwrap();

    let ts = q.push(b"inspect-me").unwrap();
    let row = c.poll(Duration::from_millis(100)).unwrap().unwrap();
    assert_eq!(row.0, ts);
    c.flush().unwrap();

    let inspect = c.inspect().unwrap();
    assert_eq!(inspect.info.group_name, "inspectors");
    assert_eq!(inspect.info.running_expired_seconds, 7);
    assert_eq!(inspect.info.max_retry_count, 2);
    assert_eq!(inspect.state.pending_entries.len(), 1);
    assert_eq!(inspect.state.pending_entries[0].timestamp, ts);
    assert_eq!(inspect.state.pending_entries[0].status, 0);
    assert_eq!(inspect.state.pending_entries[0].retry_count, 0);
    store.close().unwrap();
}

#[test]
fn t45_3_consumer_close_invalidates_same_group_handles_and_releases_pending() {
    use timslite::{Store, StoreConfig, TmslError};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let h = store
        .create_dataset(
            "t45close",
            "events",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();
    let q = h.open_queue().unwrap();
    let c1 = q.open_consumer("workers").unwrap();
    let c2 = q.open_consumer("workers").unwrap();

    let ts = q.push(b"retry-after-close").unwrap();
    let row = c1.poll(Duration::from_millis(100)).unwrap().unwrap();
    assert_eq!(row.0, ts);

    c1.close().unwrap();
    assert!(matches!(
        c1.poll(Duration::from_millis(0)).unwrap_err(),
        TmslError::QueueClosed(_)
    ));
    assert!(matches!(c2.ack(ts).unwrap_err(), TmslError::QueueClosed(_)));

    let reopened = q.open_consumer("workers").unwrap();
    let retried = reopened.poll(Duration::from_millis(100)).unwrap().unwrap();
    assert_eq!(retried.0, ts);
    assert_eq!(retried.1, b"retry-after-close");
    store.close().unwrap();
}

#[test]
fn t45_4_drop_consumer_deletes_closed_group_state_and_recreates_fresh() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let h = store
        .create_dataset(
            "t45drop",
            "events",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("obsolete").unwrap();

    let old_ts = q.push(b"old").unwrap();
    assert_eq!(
        c.poll(Duration::from_millis(100)).unwrap().unwrap().0,
        old_ts
    );
    c.close().unwrap();
    q.drop_consumer("obsolete").unwrap();
    assert!(!h.base_dir().join("queue").join("obsolete").exists());

    let recreated = q.open_consumer("obsolete").unwrap();
    assert!(recreated.poll(Duration::from_millis(20)).unwrap().is_none());
    let new_ts = q.push(b"new").unwrap();
    let row = recreated.poll(Duration::from_millis(100)).unwrap().unwrap();
    assert_eq!(row.0, new_ts);
    assert_eq!(row.1, b"new");
    store.close().unwrap();
}

#[test]
fn t45_5_queue_close_releases_unacked_pending_for_reopen() {
    use timslite::{Store, StoreConfig, TmslError};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let h = store
        .create_dataset(
            "t45qclose",
            "events",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();
    let q = h.open_queue().unwrap();
    let c = q.open_consumer("workers").unwrap();
    let ts = q.push(b"from-queue-close").unwrap();
    assert_eq!(c.poll(Duration::from_millis(100)).unwrap().unwrap().0, ts);

    q.close().unwrap();
    assert!(matches!(
        c.poll(Duration::from_millis(0)).unwrap_err(),
        TmslError::QueueClosed(_)
    ));

    let q2 = h.open_queue().unwrap();
    let c2 = q2.open_consumer("workers").unwrap();
    let row = c2.poll(Duration::from_millis(100)).unwrap().unwrap();
    assert_eq!(row.0, ts);
    assert_eq!(row.1, b"from-queue-close");
    store.close().unwrap();
}
