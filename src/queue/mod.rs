//! Queue module: Dataset queue semantics with consumer groups.
//!
//! Provides push/poll/ack queue semantics on top of Dataset, with:
//! - Multi-consumer-group support (independent progress)
//! - Multi-consumer-instance per group (shared progress, exclusive poll)
//! - Persistent 4KB mmap state files (QSTF magic)
//! - Condvar-based wait/notify for poll
//!
//! Directory layout:
//! ```text
//! {data_dir}/{name}/{type}/queue/{group_name}
//! ```

use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use memmap2::MmapMut;

use crate::dataset::DataSet;
use crate::error::{Result, TmslError};
use crate::util::{read_i64_from_mmap, read_u16_from_mmap, read_u32_from_mmap};

// ─── Constants ───────────────────────────────────────────────────────────────

/// Queue state file magic bytes.
pub const QUEUE_STATE_MAGIC: &[u8; 4] = b"QSTF";

/// Queue state file version.
pub const QUEUE_STATE_VERSION: u32 = 1;

/// Fixed state file size (4KB).
pub const STATE_FILE_SIZE: usize = 4096;

/// Size of a single pending entry in the state file.
pub const PENDING_ENTRY_SIZE: usize = 17;

/// Maximum number of pending entries that fit in 4KB.
pub const MAX_PENDING_ENTRIES: usize = 239;

/// Status: pending (not yet acked).
pub const PENDING_STATUS_UNACKED: u8 = 0;

/// Status: acked.
pub const PENDING_STATUS_ACKED: u8 = 1;

/// Header size: magic(4) + version(4) + state_length(2) + processed_ts(8) + pending_length(2) + pending_value_size(1) = 21
pub const STATE_HEADER_SIZE: usize = 21;

/// Default pending timeout in seconds (5 minutes).
pub const DEFAULT_PENDING_TIMEOUT_SECS: u64 = 300;

// ─── PendingEntry ────────────────────────────────────────────────────────────

/// A single pending entry tracked in the consumer state file.
#[derive(Clone, Debug)]
pub struct PendingEntry {
    pub timestamp: i64,
    pub start_time: i64,
    pub status: u8,
}

impl PendingEntry {
    fn serialize_to(&self, buf: &mut [u8; PENDING_ENTRY_SIZE]) {
        buf[0..8].copy_from_slice(&self.timestamp.to_le_bytes());
        buf[8..16].copy_from_slice(&self.start_time.to_le_bytes());
        buf[16] = self.status;
    }

    fn deserialize_from(buf: &[u8; PENDING_ENTRY_SIZE]) -> Self {
        PendingEntry {
            timestamp: i64::from_le_bytes(buf[0..8].try_into().unwrap()),
            start_time: i64::from_le_bytes(buf[8..16].try_into().unwrap()),
            status: buf[16],
        }
    }
}

// ─── ConsumerStateFile ───────────────────────────────────────────────────────

/// 4KB mmap-backed state file for a single consumer group.
///
/// Layout:
/// - magic (4B) + version (4B) + state_length (2B) + processed_ts (8B)
/// - pending_length (2B) + pending_value_size (1B)
/// - pending_entries (pending_length * 17 B)
pub struct ConsumerStateFile {
    path: PathBuf,
    mmap: MmapMut,
    processed_ts: i64,
    pending_entries: Vec<PendingEntry>,
}

impl ConsumerStateFile {
    /// Open existing state file or create a new one.
    pub fn open_or_create(path: PathBuf, initial_processed_ts: i64) -> Result<Self> {
        if path.exists() {
            Self::open_existing(path)
        } else {
            Self::create_new(path, initial_processed_ts)
        }
    }

    fn create_new(path: PathBuf, initial_processed_ts: i64) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = File::create(&path)?;
        file.set_len(STATE_FILE_SIZE as u64)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        // Write header
        mmap[0..4].copy_from_slice(QUEUE_STATE_MAGIC);
        mmap[4..8].copy_from_slice(&QUEUE_STATE_VERSION.to_le_bytes());
        // state_length = 8 (processed_ts is 8 bytes)
        mmap[8..10].copy_from_slice(&8u16.to_le_bytes());
        write_i64_at(&mut mmap, 10, initial_processed_ts);
        // pending_length = 0
        mmap[18..20].copy_from_slice(&0u16.to_le_bytes());
        // pending_value_size = 17
        mmap[20] = PENDING_ENTRY_SIZE as u8;

        mmap.flush()?;

        Ok(ConsumerStateFile {
            path,
            mmap,
            processed_ts: initial_processed_ts,
            pending_entries: Vec::new(),
        })
    }

    fn open_existing(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        if &mmap[0..4] != QUEUE_STATE_MAGIC {
            return Err(TmslError::InvalidMagic);
        }
        let version = read_u32_from_mmap(&mmap, 4);
        if version != QUEUE_STATE_VERSION {
            return Err(TmslError::InvalidVersion(version as u16));
        }

        let processed_ts = read_i64_from_mmap(&mmap, 10);
        let pending_length = read_u16_from_mmap(&mmap, 18) as usize;

        let mut pending_entries = Vec::with_capacity(pending_length);
        for i in 0..pending_length {
            let offset = STATE_HEADER_SIZE + i * PENDING_ENTRY_SIZE;
            if offset + PENDING_ENTRY_SIZE > STATE_FILE_SIZE {
                return Err(TmslError::InvalidData(format!(
                    "pending entry {i} out of bounds (offset={offset})"
                )));
            }
            let mut buf = [0u8; PENDING_ENTRY_SIZE];
            buf.copy_from_slice(&mmap[offset..offset + PENDING_ENTRY_SIZE]);
            pending_entries.push(PendingEntry::deserialize_from(&buf));
        }

        Ok(ConsumerStateFile {
            path,
            mmap,
            processed_ts,
            pending_entries,
        })
    }

    /// Write in-memory state back to mmap (no fsync — unified with bg flush).
    pub fn sync_to_mmap(&mut self) -> Result<()> {
        // Update header processed_ts
        write_i64_at(&mut self.mmap, 10, self.processed_ts);
        // Update pending_length
        let len = self.pending_entries.len() as u16;
        self.mmap[18..20].copy_from_slice(&len.to_le_bytes());
        // Write pending entries
        for (i, entry) in self.pending_entries.iter().enumerate() {
            let offset = STATE_HEADER_SIZE + i * PENDING_ENTRY_SIZE;
            if offset + PENDING_ENTRY_SIZE > STATE_FILE_SIZE {
                return Err(TmslError::PendingFull(format!("{}", self.path.display())));
            }
            let mut buf = [0u8; PENDING_ENTRY_SIZE];
            entry.serialize_to(&mut buf);
            self.mmap[offset..offset + PENDING_ENTRY_SIZE].copy_from_slice(&buf);
        }
        // Zero out unused pending area
        let used_end = STATE_HEADER_SIZE + self.pending_entries.len() * PENDING_ENTRY_SIZE;
        let remaining = STATE_FILE_SIZE - used_end;
        if remaining > 0 {
            let zeros = vec![0u8; remaining];
            self.mmap[used_end..STATE_FILE_SIZE].copy_from_slice(&zeros);
        }
        Ok(())
    }

    /// Flush mmap to disk (MS_SYNC) — called by background flush task.
    pub fn flush(&self) -> Result<()> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Add a pending entry. Returns error if capacity reached.
    pub fn add_pending(&mut self, entry: PendingEntry) -> Result<()> {
        if self.pending_entries.len() >= MAX_PENDING_ENTRIES {
            return Err(TmslError::PendingFull(format!(
                "{} ({} entries)",
                self.path.display(),
                self.pending_entries.len()
            )));
        }
        self.pending_entries.push(entry);
        Ok(())
    }

    /// Find pending entry by timestamp.
    pub fn find_pending(&self, timestamp: i64) -> Option<&PendingEntry> {
        self.pending_entries
            .iter()
            .find(|e| e.timestamp == timestamp)
    }

    /// Check if timestamp is in pending (any status).
    pub fn is_in_pending(&self, timestamp: i64) -> bool {
        self.pending_entries
            .iter()
            .any(|e| e.timestamp == timestamp)
    }

    /// Find the first unacked pending entry (for poll allocation).
    pub fn find_first_unacked(&self) -> Option<&PendingEntry> {
        self.pending_entries
            .iter()
            .find(|e| e.status == PENDING_STATUS_UNACKED)
    }

    /// Mark a pending entry as acked by timestamp.
    pub fn ack_pending(&mut self, timestamp: i64) -> Result<()> {
        let entry = self
            .pending_entries
            .iter_mut()
            .find(|e| e.timestamp == timestamp)
            .ok_or_else(|| {
                TmslError::NotFound(format!("pending entry for timestamp {}", timestamp))
            })?;
        entry.status = PENDING_STATUS_ACKED;
        Ok(())
    }

    /// Scan consecutive acked entries from the beginning and update processed_ts.
    /// Returns the number of entries cleaned up.
    pub fn cleanup_acked(&mut self) -> usize {
        let mut count = 0;
        for entry in &self.pending_entries {
            if entry.status == PENDING_STATUS_ACKED {
                count += 1;
            } else {
                break;
            }
        }
        if count > 0 {
            // Update processed_ts to the max consecutive acked timestamp
            if let Some(entry) = self.pending_entries.get(count - 1) {
                if entry.timestamp > self.processed_ts {
                    self.processed_ts = entry.timestamp;
                }
            }
            self.pending_entries.drain(..count);
        }
        count
    }

    /// Remove pending entries that have been waiting longer than timeout_secs.
    pub fn cleanup_timeout(&mut self, timeout_secs: u64) -> usize {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let before = self.pending_entries.len();
        self.pending_entries
            .retain(|e| e.start_time == 0 || (now - e.start_time) < timeout_secs as i64);
        before - self.pending_entries.len()
    }

    /// Get the next timestamp to poll (processed_ts + 1, or initial if no records).
    pub fn next_poll_ts(&self) -> i64 {
        if self.processed_ts > 0 {
            self.processed_ts + 1
        } else {
            1
        }
    }

    pub fn processed_ts(&self) -> i64 {
        self.processed_ts
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn pending_count(&self) -> usize {
        self.pending_entries.len()
    }

    pub fn pending_entries(&self) -> &[PendingEntry] {
        &self.pending_entries
    }

    /// Write in-memory state back to mmap (alias for sync_to_mmap).
    pub fn sync(&mut self) -> Result<()> {
        self.sync_to_mmap()
    }
}

fn read_i64_at(mmap: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(mmap[offset..offset + 8].try_into().unwrap())
}

fn write_i64_at(mmap: &mut MmapMut, offset: usize, val: i64) {
    mmap[offset..offset + 8].copy_from_slice(&val.to_le_bytes());
}

// ─── QueueInner ──────────────────────────────────────────────────────────────

/// Shared internal state for a dataset queue.
pub(crate) struct QueueInner {
    consumers: HashMap<String, Arc<Mutex<ConsumerStateFile>>>,
    closed: Arc<AtomicBool>,
}

impl QueueInner {
    pub(crate) fn new() -> Self {
        QueueInner {
            consumers: HashMap::new(),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    pub(crate) fn consumers(&self) -> &HashMap<String, Arc<Mutex<ConsumerStateFile>>> {
        &self.consumers
    }

    pub(crate) fn consumers_mut(&mut self) -> &mut HashMap<String, Arc<Mutex<ConsumerStateFile>>> {
        &mut self.consumers
    }
}

// ─── DatasetQueue ────────────────────────────────────────────────────────────

/// Queue handle for a dataset (Clone-safe, singleton per dataset).
pub struct DatasetQueue {
    pub(crate) dataset: Arc<Mutex<DataSet>>,
    pub(crate) inner: Arc<Mutex<QueueInner>>,
    pub(crate) notify: Arc<(Mutex<bool>, Condvar)>,
}

impl Clone for DatasetQueue {
    fn clone(&self) -> Self {
        DatasetQueue {
            dataset: Arc::clone(&self.dataset),
            inner: Arc::clone(&self.inner),
            notify: Arc::clone(&self.notify),
        }
    }
}

impl fmt::Debug for DatasetQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatasetQueue").finish_non_exhaustive()
    }
}

impl DatasetQueue {
    /// Open or create a consumer group and return a consumer handle.
    pub fn open_consumer(&self, group_name: &str) -> Result<DatasetQueueConsumer> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?;

        if inner.is_closed() {
            return Err(TmslError::QueueClosed("queue is closed".into()));
        }

        let queue_dir = {
            let ds = self
                .dataset
                .lock()
                .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
            ds.queue_dir()
        };

        let state_file = if inner.consumers.contains_key(group_name) {
            Arc::clone(&inner.consumers[group_name])
        } else {
            let state_path = queue_dir.join(group_name);
            // Determine initial processed_ts from the dataset
            let initial_ts = {
                let ds = self
                    .dataset
                    .lock()
                    .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
                ds.latest_written_timestamp()
            };
            let sf = Arc::new(Mutex::new(ConsumerStateFile::open_or_create(
                state_path, initial_ts,
            )?));
            inner
                .consumers
                .insert(group_name.to_string(), Arc::clone(&sf));
            sf
        };

        Ok(DatasetQueueConsumer {
            group_name: group_name.to_string(),
            state_file,
            notify: Arc::clone(&self.notify),
            dataset: Arc::clone(&self.dataset),
            closed: Arc::clone(&inner.closed),
        })
    }

    /// Drop (close and remove) a consumer group.
    pub fn drop_consumer(&self, group_name: &str) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?;

        // Sync state file before dropping
        if let Some(sf) = inner.consumers.get(group_name) {
            let mut guard = sf
                .lock()
                .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
            guard.sync()?;
        }

        let removed = inner.consumers.remove(group_name);
        if removed.is_none() {
            return Err(TmslError::ConsumerGroupNotFound(group_name.to_string()));
        }

        // Try to delete the state file
        let queue_dir = {
            let ds = self
                .dataset
                .lock()
                .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
            ds.queue_dir()
        };
        let state_path = queue_dir.join(group_name);
        let _ = fs::remove_file(&state_path);

        Ok(())
    }

    /// Push data into the dataset queue.
    ///
    /// Assigns an auto-increment timestamp (latest_written_timestamp + 1),
    /// writes to the dataset, and notifies waiting consumers.
    pub fn push(&self, data: &[u8]) -> Result<i64> {
        if self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?
            .is_closed()
        {
            return Err(TmslError::QueueClosed("queue is closed".into()));
        }

        let mut ds = self
            .dataset
            .lock()
            .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;

        // Auto-increment timestamp
        let timestamp = ds.latest_written_timestamp() + 1;
        ds.write(timestamp, data)?;

        // Notify waiting consumers (only on normal write, ts > old_latest)
        let (lock, cvar) = (&self.notify.0, &self.notify.1);
        let mut notified = lock
            .lock()
            .map_err(|_| TmslError::InvalidData("notify mutex poisoned".into()))?;
        *notified = true;
        cvar.notify_all();

        Ok(timestamp)
    }

    /// Close the queue (marks as closed, drops all consumers).
    pub fn close(&self) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?;

        if inner.is_closed() {
            return Ok(());
        }

        // Sync all consumer state files
        for sf in inner.consumers.values() {
            if let Ok(mut guard) = sf.lock() {
                let _ = guard.sync();
            }
        }

        inner.close();
        inner.consumers.clear();

        // Notify any waiting polls
        let (lock, cvar) = (&self.notify.0, &self.notify.1);
        let mut notified = lock
            .lock()
            .map_err(|_| TmslError::InvalidData("notify mutex poisoned".into()))?;
        *notified = true;
        cvar.notify_all();

        Ok(())
    }
}

// ─── DatasetQueueConsumer ────────────────────────────────────────────────────

/// Consumer handle for a specific consumer group.
///
/// Multiple consumers can exist for the same group, sharing progress
/// via the shared state file. poll() is exclusive via state file mutex.
pub struct DatasetQueueConsumer {
    group_name: String,
    state_file: Arc<Mutex<ConsumerStateFile>>,
    notify: Arc<(Mutex<bool>, Condvar)>,
    dataset: Arc<Mutex<DataSet>>,
    closed: Arc<AtomicBool>,
}

impl Clone for DatasetQueueConsumer {
    fn clone(&self) -> Self {
        DatasetQueueConsumer {
            group_name: self.group_name.clone(),
            state_file: Arc::clone(&self.state_file),
            notify: Arc::clone(&self.notify),
            dataset: Arc::clone(&self.dataset),
            closed: Arc::clone(&self.closed),
        }
    }
}

impl fmt::Debug for DatasetQueueConsumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatasetQueueConsumer")
            .field("group_name", &self.group_name)
            .finish_non_exhaustive()
    }
}

impl DatasetQueueConsumer {
    /// Poll for the next unacked record.
    ///
    /// - If there's already an unacked pending entry, return it immediately.
    /// - Otherwise, read from the dataset starting at processed_ts + 1.
    /// - If no data is available, wait up to `timeout` for a push notification.
    ///
    /// Returns `Ok(Some((timestamp, data)))` if a record is found,
    /// `Ok(None)` if timeout expires, or `Err(QueueClosed)` if the queue was closed.
    pub fn poll(&self, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(TmslError::QueueClosed("queue is closed".into()));
        }

        // Check for existing unacked pending entry
        {
            let sf = self
                .state_file
                .lock()
                .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
            if let Some(entry) = sf.find_first_unacked() {
                let ts = entry.timestamp;
                drop(sf);
                return self.read_record_at(ts);
            }
        }

        // No unacked pending — try to read next record from dataset
        let next_ts = {
            let sf = self
                .state_file
                .lock()
                .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
            sf.next_poll_ts()
        };

        let ds_result = {
            let mut ds = self
                .dataset
                .lock()
                .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
            ds.read(next_ts, None)
        };

        match ds_result {
            Ok(Some((ts, data))) => {
                // Add to pending
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                {
                    let mut sf = self
                        .state_file
                        .lock()
                        .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
                    let entry = PendingEntry {
                        timestamp: ts,
                        start_time: now,
                        status: PENDING_STATUS_UNACKED,
                    };
                    sf.add_pending(entry)?;
                }
                Ok(Some((ts, data)))
            }
            Ok(None) => {
                // No data available — wait for notification or timeout
                self.wait_for_data(timeout)
            }
            Err(e) => Err(e),
        }
    }

    fn wait_for_data(&self, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>> {
        let (lock, cvar) = (&self.notify.0, &self.notify.1);
        let mut notified = lock
            .lock()
            .map_err(|_| TmslError::InvalidData("notify mutex poisoned".into()))?;

        let deadline = Instant::now() + timeout;

        loop {
            if self.closed.load(Ordering::SeqCst) {
                return Err(TmslError::QueueClosed("queue is closed".into()));
            }

            if *notified {
                *notified = false;
                drop(notified);

                // Re-check for unacked pending
                {
                    let sf = self
                        .state_file
                        .lock()
                        .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
                    if let Some(entry) = sf.find_first_unacked() {
                        let ts = entry.timestamp;
                        drop(sf);
                        return self.read_record_at(ts);
                    }
                }

                // Try to read next record
                let next_ts = {
                    let sf = self
                        .state_file
                        .lock()
                        .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
                    sf.next_poll_ts()
                };

                let ds_result = {
                    let mut ds = self
                        .dataset
                        .lock()
                        .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
                    ds.read(next_ts, None)
                };

                match ds_result {
                    Ok(Some((ts, data))) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() as i64;
                        {
                            let mut sf = self.state_file.lock().map_err(|_| {
                                TmslError::InvalidData("state file mutex poisoned".into())
                            })?;
                            sf.add_pending(PendingEntry {
                                timestamp: ts,
                                start_time: now,
                                status: PENDING_STATUS_UNACKED,
                            })?;
                        }
                        return Ok(Some((ts, data)));
                    }
                    Ok(None) => {
                        notified = lock
                            .lock()
                            .map_err(|_| TmslError::InvalidData("notify mutex poisoned".into()))?;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }

            let remaining = match deadline.checked_duration_since(Instant::now()) {
                Some(d) => d,
                None => return Ok(None), // Timeout
            };

            let (new_notified, timeout_result) = cvar.wait_timeout(notified, remaining).unwrap();
            notified = new_notified;
            if timeout_result.timed_out() {
                // One final check after timeout
                drop(notified);
                {
                    let sf = self
                        .state_file
                        .lock()
                        .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
                    if let Some(entry) = sf.find_first_unacked() {
                        let ts = entry.timestamp;
                        drop(sf);
                        return self.read_record_at(ts);
                    }
                }
                return Ok(None);
            }
        }
    }

    fn read_record_at(&self, timestamp: i64) -> Result<Option<(i64, Vec<u8>)>> {
        let mut ds = self
            .dataset
            .lock()
            .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
        ds.read(timestamp, None)
    }

    /// Ack a previously polled record.
    pub fn ack(&self, timestamp: i64) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(TmslError::QueueClosed("queue is closed".into()));
        }

        let mut sf = self
            .state_file
            .lock()
            .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;

        sf.ack_pending(timestamp)?;
        sf.cleanup_acked();
        Ok(())
    }
}

// ─── DataSet integration helpers ─────────────────────────────────────────────

/// Queue directory path for a dataset.
pub(crate) fn queue_dir_for(base_dir: &Path) -> PathBuf {
    base_dir.join("queue")
}

/// Flush all consumer state files for a dataset (called by background task).
pub(crate) fn flush_queue_state_files(inner: &Arc<Mutex<QueueInner>>) -> Result<()> {
    let guard = inner
        .lock()
        .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?;

    for sf in guard.consumers.values() {
        if let Ok(mut state) = sf.lock() {
            // Cleanup timeout entries first
            state.cleanup_timeout(DEFAULT_PENDING_TIMEOUT_SECS);
            state.sync_to_mmap()?;
            state.flush()?;
        }
    }
    Ok(())
}
