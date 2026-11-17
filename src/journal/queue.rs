use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::error::{Result, TmslError};
use crate::journal::log::JournalLog;
use crate::queue::{
    now_unix_seconds, ConsumerStateFile, QueueConsumerConfig, QueueNotifier, QueuePollCallback,
    QueuePollCallbackSlot,
};
use crate::util::{is_path_safe_component, PATH_COMPONENT_MAX_LEN};

fn validate_consumer_group_name(group_name: &str) -> Result<()> {
    if is_path_safe_component(group_name) {
        Ok(())
    } else {
        Err(TmslError::InvalidData(format!(
            "journal queue consumer group_name must match ^[0-9A-Za-z_-]+$ and be at most {PATH_COMPONENT_MAX_LEN} bytes"
        )))
    }
}

struct JournalQueueInner {
    consumers: HashMap<String, Arc<Mutex<ConsumerStateFile>>>,
    consumer_configs: HashMap<String, QueueConsumerConfig>,
    closed: Arc<AtomicBool>,
}

impl JournalQueueInner {
    fn new() -> Self {
        Self {
            consumers: HashMap::new(),
            consumer_configs: HashMap::new(),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Clone)]
pub struct JournalQueue {
    log: Arc<Mutex<JournalLog>>,
    queue_dir: PathBuf,
    inner: Arc<Mutex<JournalQueueInner>>,
    notify: Arc<QueueNotifier>,
}

impl JournalQueue {
    pub(crate) fn new(log: Arc<Mutex<JournalLog>>, queue_dir: PathBuf) -> Self {
        Self {
            log,
            queue_dir,
            inner: Arc::new(Mutex::new(JournalQueueInner::new())),
            notify: Arc::new(QueueNotifier::new()),
        }
    }

    pub fn open_consumer(&self, group_name: &str) -> Result<JournalQueueConsumer> {
        self.open_consumer_with_config(group_name, QueueConsumerConfig::default())
    }

    pub fn open_consumer_with_config(
        &self,
        group_name: &str,
        config: QueueConsumerConfig,
    ) -> Result<JournalQueueConsumer> {
        validate_consumer_group_name(group_name)?;
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("journal queue mutex poisoned".into()))?;
        if inner.closed.load(Ordering::SeqCst) {
            return Err(TmslError::QueueClosed("journal queue is closed".into()));
        }
        if let Some(existing) = inner.consumer_configs.get(group_name) {
            if *existing != config {
                return Err(TmslError::InvalidData(format!(
                    "journal consumer group {group_name} is already open with a different config"
                )));
            }
        }
        let state_file = if let Some(existing) = inner.consumers.get(group_name) {
            Arc::clone(existing)
        } else {
            let initial_sequence = self
                .log
                .lock()
                .map_err(|_| TmslError::InvalidData("journal log mutex poisoned".into()))?
                .latest_sequence()
                .unwrap_or(0);
            let state_path = self.queue_dir.join(group_name);
            let state = Arc::new(Mutex::new(ConsumerStateFile::open_or_create(
                state_path,
                initial_sequence,
            )?));
            inner
                .consumers
                .insert(group_name.to_string(), Arc::clone(&state));
            inner
                .consumer_configs
                .insert(group_name.to_string(), config);
            state
        };
        let callback_slot = Arc::new(Mutex::new(None));
        self.notify.register_callback_slot(&callback_slot)?;

        Ok(JournalQueueConsumer {
            state_file,
            config,
            log: Arc::clone(&self.log),
            notify: Arc::clone(&self.notify),
            closed: Arc::clone(&inner.closed),
            poll_callback: callback_slot,
        })
    }

    pub fn close(&self) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("journal queue mutex poisoned".into()))?;
        if inner.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        for state in inner.consumers.values() {
            if let Ok(mut guard) = state.lock() {
                let _ = guard.sync();
                let _ = guard.flush();
            }
        }
        inner.consumers.clear();
        inner.consumer_configs.clear();
        drop(inner);
        self.notify_waiters();
        Ok(())
    }

    pub(crate) fn notify_record_appended(&self) {
        let _ = self.notify.notify_data_available();
    }

    fn notify_waiters(&self) {
        let _ = self.notify.notify_waiters();
    }

    pub(crate) fn flush_state_files(&self) -> Result<()> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("journal queue mutex poisoned".into()))?;
        for state in inner.consumers.values() {
            let mut guard = state
                .lock()
                .map_err(|_| TmslError::InvalidData("journal state file mutex poisoned".into()))?;
            guard.sync()?;
            guard.flush()?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct JournalQueueConsumer {
    state_file: Arc<Mutex<ConsumerStateFile>>,
    config: QueueConsumerConfig,
    log: Arc<Mutex<JournalLog>>,
    notify: Arc<QueueNotifier>,
    closed: Arc<AtomicBool>,
    poll_callback: QueuePollCallbackSlot,
}

impl JournalQueueConsumer {
    pub fn poll(&self, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(TmslError::QueueClosed("journal queue is closed".into()));
        }
        if let Some(row) = self.try_poll_available()? {
            return Ok(Some(row));
        }
        if timeout.is_zero() {
            return Ok(None);
        }
        let deadline = Instant::now() + timeout;
        let (lock, cvar) = self.notify.wait_parts();
        let mut notified = lock
            .lock()
            .map_err(|_| TmslError::InvalidData("journal notify mutex poisoned".into()))?;
        loop {
            if self.closed.load(Ordering::SeqCst) {
                return Err(TmslError::QueueClosed("journal queue is closed".into()));
            }
            if *notified {
                *notified = false;
                drop(notified);
                if let Some(row) = self.try_poll_available()? {
                    return Ok(Some(row));
                }
                notified = lock
                    .lock()
                    .map_err(|_| TmslError::InvalidData("journal notify mutex poisoned".into()))?;
                continue;
            }
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                drop(notified);
                return self.try_poll_available();
            };
            let (guard, wait_result) = cvar.wait_timeout(notified, remaining).unwrap();
            notified = guard;
            if wait_result.timed_out() {
                drop(notified);
                return self.try_poll_available();
            }
        }
    }

    pub fn ack(&self, sequence: i64) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(TmslError::QueueClosed("journal queue is closed".into()));
        }
        let mut state = self
            .state_file
            .lock()
            .map_err(|_| TmslError::InvalidData("journal state file mutex poisoned".into()))?;
        state.ack_pending(sequence)?;
        state.cleanup_acked();
        state.sync()?;
        Ok(())
    }

    pub fn poll_callback(&self, callback: Option<QueuePollCallback>) -> Result<()> {
        let mut slot = self.poll_callback.lock().map_err(|_| {
            TmslError::InvalidData("journal queue callback slot mutex poisoned".into())
        })?;
        if callback.is_some() && slot.is_some() {
            return Err(TmslError::InvalidData(
                "journal queue poll callback is already registered".into(),
            ));
        }
        *slot = callback;
        Ok(())
    }

    fn try_poll_available(&self) -> Result<Option<(i64, Vec<u8>)>> {
        let now = now_unix_seconds();

        // Retry an expired pending delivery. Commit the retry attempt only
        // after the unlocked log read succeeds.
        let retry_scan = {
            let state = self
                .state_file
                .lock()
                .map_err(|_| TmslError::InvalidData("journal state file mutex poisoned".into()))?;
            state.peek_retryable_pending(self.config, now)
        };
        if let Some(sequence) = retry_scan.timestamp {
            match self.read_sequence(sequence) {
                Ok(Some((seq, data))) => {
                    if self.commit_retry_claim(Some(sequence), now)? {
                        return Ok(Some((seq, data)));
                    }
                    // A peer in the same group claimed this retry first.
                }
                Ok(None) => {
                    // The sequence is gone; consume the retry attempt and keep
                    // scanning instead of returning a false `None`.
                    self.commit_retry_claim(Some(sequence), now)?;
                }
                Err(err) => return Err(err),
            }
        } else if retry_scan.has_exhausted {
            self.commit_retry_claim(None, now)?;
        }

        loop {
            let next_sequence = self
                .log
                .lock()
                .map_err(|_| TmslError::InvalidData("journal log mutex poisoned".into()))?
                .next_sequence();

            let candidate = {
                let state = self.state_file.lock().map_err(|_| {
                    TmslError::InvalidData("journal state file mutex poisoned".into())
                })?;
                let mut candidate = state.next_poll_ts();
                while candidate < next_sequence && state.is_in_pending(candidate) {
                    candidate = candidate.saturating_add(1);
                }
                candidate
            };
            if candidate >= next_sequence {
                return Ok(None);
            }

            // Read outside the state lock, then revalidate + claim.
            let Some((sequence, data)) = self.read_sequence(candidate)? else {
                // Journal sequences are contiguous; do not skip ahead.
                return Ok(None);
            };
            let claimed = {
                let mut state = self.state_file.lock().map_err(|_| {
                    TmslError::InvalidData("journal state file mutex poisoned".into())
                })?;
                // Recheck close under the state-file lock: a close that raced
                // the unlocked log read must not be followed by a claim.
                let claimed = self.try_claim_pending_checked(&mut state, candidate, now)?;
                if claimed {
                    state.sync()?;
                }
                claimed
            };
            if claimed {
                return Ok(Some((sequence, data)));
            }
            // A peer claimed the candidate; recompute the next candidate.
        }
    }

    fn try_claim_pending_checked(
        &self,
        state: &mut ConsumerStateFile,
        sequence: i64,
        now: i64,
    ) -> Result<bool> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(TmslError::QueueClosed("journal queue is closed".into()));
        }
        state.try_claim_pending(sequence, now)
    }

    fn commit_retry_claim(&self, sequence: Option<i64>, now: i64) -> Result<bool> {
        let mut state = self
            .state_file
            .lock()
            .map_err(|_| TmslError::InvalidData("journal state file mutex poisoned".into()))?;
        if self.closed.load(Ordering::SeqCst) {
            return Err(TmslError::QueueClosed("journal queue is closed".into()));
        }
        let commit = state.commit_retryable_pending(self.config, now, sequence)?;
        if commit.changed {
            state.sync()?;
        }
        Ok(commit.claimed)
    }

    fn read_sequence(&self, sequence: i64) -> Result<Option<(i64, Vec<u8>)>> {
        self.log
            .lock()
            .map_err(|_| TmslError::InvalidData("journal log mutex poisoned".into()))?
            .read(sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StoreConfig;
    use crate::journal::log::JournalLog;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "timslite_journal_queue_{name}_{:?}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn make_log(name: &str) -> (Arc<Mutex<JournalLog>>, std::path::PathBuf) {
        let dir = temp_dir(name);
        let config = StoreConfig {
            data_segment_size: 4096,
            initial_data_segment_size: 4096,
            ..StoreConfig::default()
        };
        let log = JournalLog::open_or_create(dir.clone(), &config).unwrap();
        (Arc::new(Mutex::new(log)), dir)
    }

    #[test]
    fn journal_queue_new_consumer_starts_after_current_latest() {
        let (log, dir) = make_log("starts_after_latest");
        log.lock().unwrap().append(b"existing").unwrap();
        let queue = JournalQueue::new(log.clone(), dir.join("queue"));
        let consumer = queue.open_consumer("group1").unwrap();

        assert!(consumer.poll(Duration::from_millis(1)).unwrap().is_none());

        let seq = log.lock().unwrap().append(b"next").unwrap();
        queue.notify_record_appended();
        let row = consumer.poll(Duration::from_millis(100)).unwrap().unwrap();
        assert_eq!(row.0, seq);
        assert_eq!(row.1, b"next");
    }

    #[test]
    fn journal_queue_polls_realtime_sequence_one_based() {
        let (log, dir) = make_log("one_based");
        let queue = JournalQueue::new(log.clone(), dir.join("queue"));
        let consumer = queue.open_consumer("group1").unwrap();

        assert_eq!(log.lock().unwrap().append(b"first").unwrap(), 1);
        queue.notify_record_appended();

        let row = consumer.poll(Duration::from_millis(100)).unwrap().unwrap();
        assert_eq!(row.0, 1);
        assert_eq!(row.1, b"first");
    }

    #[test]
    fn journal_claim_recheck_rejects_closed_queue() {
        let (log, dir) = make_log("closed_claim");
        let queue = JournalQueue::new(log.clone(), dir.join("queue"));
        let consumer = queue.open_consumer("group").unwrap();
        log.lock().unwrap().append(b"payload").unwrap();

        // Close only after data exists so the poll's unlocked log read can
        // complete before the locked claim recheck observes the closed flag.
        queue.close().unwrap();
        let err = consumer.try_poll_available().unwrap_err();
        assert!(matches!(err, TmslError::QueueClosed(_)));

        let state = consumer.state_file.lock().unwrap();
        assert!(!state.is_in_pending(1));
        drop(state);

        // The retry commit path must reject close under the same lock too.
        let err = consumer
            .commit_retry_claim(Some(1), now_unix_seconds())
            .unwrap_err();
        assert!(matches!(err, TmslError::QueueClosed(_)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn journal_queue_same_group_concurrent_claims_are_unique() {
        use std::collections::HashSet;
        use std::sync::mpsc;
        use std::sync::{Arc, Barrier};
        use std::thread;

        const CONSUMERS: usize = 4;
        const RECORDS: i64 = 200;

        let (log, dir) = make_log("concurrent_unique");
        let queue = JournalQueue::new(log.clone(), dir.join("queue"));
        // Consumers must be opened before the records exist: a fresh group
        // starts at the current latest sequence.
        let consumers: Vec<_> = (0..CONSUMERS)
            .map(|_| queue.open_consumer("same_group").unwrap())
            .collect();
        for i in 0..RECORDS {
            log.lock()
                .unwrap()
                .append(format!("record-{i}").as_bytes())
                .unwrap();
        }

        let barrier = Arc::new(Barrier::new(CONSUMERS));
        let (done_tx, done_rx) = mpsc::channel();
        let mut handles = Vec::new();
        for consumer in consumers {
            let barrier = Arc::clone(&barrier);
            let done_tx = done_tx.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                let mut seen = Vec::new();
                loop {
                    match consumer.poll(Duration::ZERO) {
                        Ok(Some((sequence, _))) => {
                            seen.push(sequence);
                            // Acking is best-effort here: a duplicate claim may
                            // already have been cleaned up by the winning peer.
                            let _ = consumer.ack(sequence);
                        }
                        Ok(None) => break,
                        Err(err) => panic!("journal poll failed: {err:?}"),
                    }
                }
                let _ = done_tx.send(seen);
            }));
        }
        drop(done_tx);

        let mut claimed = Vec::new();
        for _ in 0..CONSUMERS {
            match done_rx.recv_timeout(Duration::from_secs(10)) {
                Ok(seen) => claimed.extend(seen),
                Err(_) => panic!("journal concurrent claim test timed out"),
            }
        }
        for handle in handles {
            handle.join().expect("journal claim worker panicked");
        }

        let unique: HashSet<i64> = claimed.iter().copied().collect();
        assert_eq!(
            claimed.len(),
            RECORDS as usize,
            "each journal record must be claimed exactly once"
        );
        assert_eq!(unique.len(), RECORDS as usize, "duplicate claims detected");
        for expected in 1..=RECORDS {
            assert!(unique.contains(&expected), "missing sequence {expected}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn journal_queue_ack_persists_processed_sequence() {
        let (log, dir) = make_log("ack_persists");
        let queue_dir = dir.join("queue");
        let queue = JournalQueue::new(log.clone(), queue_dir.clone());
        let consumer = queue.open_consumer("group1").unwrap();
        log.lock().unwrap().append(b"first").unwrap();
        queue.notify_record_appended();
        let row = consumer.poll(Duration::from_millis(100)).unwrap().unwrap();
        consumer.ack(row.0).unwrap();
        queue.flush_state_files().unwrap();
        drop(consumer);
        drop(queue);

        let queue = JournalQueue::new(log, queue_dir);
        let consumer = queue.open_consumer("group1").unwrap();

        assert!(consumer.poll(Duration::from_millis(1)).unwrap().is_none());
    }
}
