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
use std::fs::{self, OpenOptions};
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use memmap2::MmapMut;

use crate::dataset::DataSet;
use crate::error::{Result, TmslError};
use crate::util::{
    is_path_safe_component, read_i64_from_mmap, read_u16_from_mmap, read_u32_from_mmap,
    PATH_COMPONENT_MAX_LEN,
};

/// Queue state file magic bytes.
pub const QUEUE_STATE_MAGIC: &[u8; 4] = b"QSTF";

/// Queue state file version.
pub const QUEUE_STATE_VERSION: u32 = 1;

/// Fixed state file size (4KB).
pub const STATE_FILE_SIZE: usize = 4096;

/// Size of a single pending entry in the state file.
pub const PENDING_ENTRY_SIZE: usize = 18;

/// Maximum number of pending entries that fit in 4KB.
pub const MAX_PENDING_ENTRIES: usize = (STATE_FILE_SIZE - STATE_HEADER_SIZE) / PENDING_ENTRY_SIZE;

/// Status: pending (not yet acked).
pub const PENDING_STATUS_UNACKED: u8 = 0;

/// Status: acked.
pub const PENDING_STATUS_ACKED: u8 = 1;

/// Header size: magic(4) + version(4) + state_length(2) + processed_ts(8) + pending_length(2) + pending_value_size(1) = 21
pub const STATE_HEADER_SIZE: usize = 21;

/// Default visibility timeout in seconds for unacked pending entries.
pub const DEFAULT_RUNNING_EXPIRED_SECONDS: u16 = 900;

/// Default retry limit. Zero means unlimited.
pub const DEFAULT_MAX_RETRY_COUNT: u8 = 3;

/// Lightweight callback invoked after queue data notifications.
///
/// The callback is best-effort and only intended to wake external processing.
/// It does not participate in queue state, pending, retry, or ack semantics.
pub type QueuePollCallback = Arc<dyn Fn() + Send + Sync + 'static>;

pub(crate) type QueuePollCallbackSlot = Arc<Mutex<Option<QueuePollCallback>>>;

/// Shared queue notification primitive for blocking polls and lightweight callbacks.
pub(crate) struct QueueNotifier {
    notified: Mutex<bool>,
    cvar: Condvar,
    callbacks: Mutex<Vec<Weak<Mutex<Option<QueuePollCallback>>>>>,
}

impl QueueNotifier {
    pub(crate) fn new() -> Self {
        Self {
            notified: Mutex::new(false),
            cvar: Condvar::new(),
            callbacks: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn wait_parts(&self) -> (&Mutex<bool>, &Condvar) {
        (&self.notified, &self.cvar)
    }

    pub(crate) fn register_callback_slot(&self, slot: &QueuePollCallbackSlot) -> Result<()> {
        self.callbacks
            .lock()
            .map_err(|_| TmslError::InvalidData("queue callback mutex poisoned".into()))?
            .push(Arc::downgrade(slot));
        Ok(())
    }

    pub(crate) fn notify_waiters(&self) -> Result<()> {
        let mut notified = self
            .notified
            .lock()
            .map_err(|_| TmslError::InvalidData("notify mutex poisoned".into()))?;
        *notified = true;
        self.cvar.notify_all();
        Ok(())
    }

    pub(crate) fn notify_data_available(&self) -> Result<()> {
        self.notify_waiters()?;
        self.run_callbacks();
        Ok(())
    }

    pub(crate) fn notify_data_available_best_effort(&self) {
        if self.notify_waiters().is_ok() {
            self.run_callbacks();
        }
    }

    fn run_callbacks(&self) {
        let mut ready = Vec::new();
        let Ok(mut callbacks) = self.callbacks.lock() else {
            return;
        };
        callbacks.retain(|weak| {
            let Some(slot) = weak.upgrade() else {
                return false;
            };
            if let Ok(guard) = slot.lock() {
                if let Some(callback) = guard.as_ref() {
                    ready.push(Arc::clone(callback));
                }
            }
            true
        });
        drop(callbacks);

        for callback in ready {
            if catch_unwind(AssertUnwindSafe(|| callback())).is_err() {
                log::warn!("[queue] poll callback panicked");
            }
        }
    }
}

fn validate_consumer_group_name(group_name: &str) -> Result<()> {
    if is_path_safe_component(group_name) {
        Ok(())
    } else {
        Err(TmslError::InvalidData(format!(
            "queue consumer group_name must match ^[0-9A-Za-z_-]+$ and be at most {PATH_COMPONENT_MAX_LEN} bytes"
        )))
    }
}

/// Runtime retry/visibility settings for one consumer group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueConsumerConfig {
    pub running_expired_seconds: u16,
    pub max_retry_count: u8,
}

impl Default for QueueConsumerConfig {
    fn default() -> Self {
        Self {
            running_expired_seconds: DEFAULT_RUNNING_EXPIRED_SECONDS,
            max_retry_count: DEFAULT_MAX_RETRY_COUNT,
        }
    }
}

impl QueueConsumerConfig {
    pub fn builder() -> QueueConsumerConfigBuilder {
        QueueConsumerConfigBuilder::default()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct QueueConsumerConfigBuilder {
    running_expired_seconds: u64,
    max_retry_count: u16,
}

impl Default for QueueConsumerConfigBuilder {
    fn default() -> Self {
        let config = QueueConsumerConfig::default();
        Self {
            running_expired_seconds: config.running_expired_seconds as u64,
            max_retry_count: config.max_retry_count as u16,
        }
    }
}

impl QueueConsumerConfigBuilder {
    pub fn running_expired_seconds(mut self, seconds: u64) -> Self {
        self.running_expired_seconds = seconds;
        self
    }

    pub fn max_retry_count(mut self, count: u16) -> Self {
        self.max_retry_count = count;
        self
    }

    pub fn build(self) -> Result<QueueConsumerConfig> {
        let running_expired_seconds =
            u16::try_from(self.running_expired_seconds).map_err(|_| {
                TmslError::InvalidData(format!(
                    "running_expired_seconds exceeds u16::MAX: {}",
                    self.running_expired_seconds
                ))
            })?;
        let max_retry_count = u8::try_from(self.max_retry_count).map_err(|_| {
            TmslError::InvalidData(format!(
                "max_retry_count exceeds u8::MAX: {}",
                self.max_retry_count
            ))
        })?;
        Ok(QueueConsumerConfig {
            running_expired_seconds,
            max_retry_count,
        })
    }
}

/// A single pending entry tracked in the consumer state file.
#[derive(Clone, Debug)]
pub struct PendingEntry {
    pub timestamp: i64,
    pub start_time: i64,
    pub status: u8,
    pub retry_count: u8,
}

/// Public pending-entry snapshot returned by `DatasetQueueConsumer::inspect`.
#[derive(Clone, Debug)]
pub struct DatasetQueueConsumerPendingEntry {
    pub timestamp: i64,
    pub start_time: i64,
    pub status: u8,
    pub retry_count: u8,
}

/// Immutable consumer-group configuration returned by inspect.
#[derive(Clone, Debug)]
pub struct DatasetQueueConsumerInfo {
    pub group_name: String,
    pub running_expired_seconds: u16,
    pub max_retry_count: u8,
}

/// Mutable consumer-group state returned by inspect.
#[derive(Clone, Debug)]
pub struct DatasetQueueConsumerState {
    pub processed_ts: i64,
    pub pending_entries: Vec<DatasetQueueConsumerPendingEntry>,
}

/// Consumer-group inspect result.
#[derive(Clone, Debug)]
pub struct DatasetQueueConsumerInspectResult {
    pub info: DatasetQueueConsumerInfo,
    pub state: DatasetQueueConsumerState,
}

impl PendingEntry {
    fn serialize_to(&self, buf: &mut [u8; PENDING_ENTRY_SIZE]) {
        buf[0..8].copy_from_slice(&self.timestamp.to_le_bytes());
        buf[8..16].copy_from_slice(&self.start_time.to_le_bytes());
        buf[16] = self.status;
        buf[17] = self.retry_count;
    }

    fn deserialize_from(buf: &[u8; PENDING_ENTRY_SIZE]) -> Self {
        PendingEntry {
            timestamp: i64::from_le_bytes(buf[0..8].try_into().unwrap()),
            start_time: i64::from_le_bytes(buf[8..16].try_into().unwrap()),
            status: buf[16],
            retry_count: buf[17],
        }
    }
}

/// 4KB mmap-backed state file for a single consumer group.
///
/// Layout:
/// - magic (4B) + version (4B) + state_length (2B) + processed_ts (8B)
/// - pending_length (2B) + pending_value_size (1B)
/// - pending_entries (pending_length * 18 B)
pub(crate) struct ConsumerStateFile {
    path: PathBuf,
    mmap: MmapMut,
    processed_ts: i64,
    pending_entries: Vec<PendingEntry>,
    is_flushed: bool,
    queued_for_flush: bool,
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

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
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
        // pending_value_size = 18
        mmap[20] = PENDING_ENTRY_SIZE as u8;

        mmap.flush()?;

        Ok(ConsumerStateFile {
            path,
            mmap,
            processed_ts: initial_processed_ts,
            pending_entries: Vec::new(),
            is_flushed: true,
            queued_for_flush: false,
        })
    }

    fn open_existing(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let file_len = file.metadata()?.len();
        if file_len != STATE_FILE_SIZE as u64 {
            return Err(TmslError::InvalidData(format!(
                "invalid queue state file length: {file_len}"
            )));
        }
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        if &mmap[0..4] != QUEUE_STATE_MAGIC {
            return Err(TmslError::InvalidMagic);
        }
        let version = read_u32_from_mmap(&mmap, 4);
        if version != QUEUE_STATE_VERSION {
            return Err(TmslError::InvalidVersion(version as u16));
        }

        let state_length = read_u16_from_mmap(&mmap, 8);
        if state_length != 8 {
            return Err(TmslError::InvalidData(format!(
                "invalid queue state length: {state_length}"
            )));
        }

        let processed_ts = read_i64_from_mmap(&mmap, 10);
        let pending_length = read_u16_from_mmap(&mmap, 18) as usize;
        let pending_value_size = mmap[20] as usize;
        if pending_value_size != PENDING_ENTRY_SIZE {
            return Err(TmslError::InvalidData(format!(
                "invalid queue pending entry size: {pending_value_size}"
            )));
        }
        if pending_length > MAX_PENDING_ENTRIES {
            return Err(TmslError::InvalidData(format!(
                "queue pending length exceeds capacity: {pending_length}"
            )));
        }

        let mut pending_entries = Vec::with_capacity(pending_length);
        let mut last_timestamp = processed_ts;
        for i in 0..pending_length {
            let offset = STATE_HEADER_SIZE + i * PENDING_ENTRY_SIZE;
            if offset + PENDING_ENTRY_SIZE > STATE_FILE_SIZE {
                return Err(TmslError::InvalidData(format!(
                    "pending entry {i} out of bounds (offset={offset})"
                )));
            }
            let mut buf = [0u8; PENDING_ENTRY_SIZE];
            buf.copy_from_slice(&mmap[offset..offset + PENDING_ENTRY_SIZE]);
            let entry = PendingEntry::deserialize_from(&buf);
            if entry.status != PENDING_STATUS_UNACKED && entry.status != PENDING_STATUS_ACKED {
                return Err(TmslError::InvalidData(format!(
                    "invalid queue pending status at entry {i}: {}",
                    entry.status
                )));
            }
            if entry.timestamp <= last_timestamp {
                return Err(TmslError::InvalidData(format!(
                    "invalid queue pending timestamp order at entry {i}: {} <= {}",
                    entry.timestamp, last_timestamp
                )));
            }
            last_timestamp = entry.timestamp;
            pending_entries.push(entry);
        }

        let mut state = ConsumerStateFile {
            path,
            mmap,
            processed_ts,
            pending_entries,
            is_flushed: true,
            queued_for_flush: false,
        };
        let mut changed = state.cleanup_acked() > 0;
        changed |= state.mark_unacked_recovery_expired();
        if changed {
            state.sync_to_mmap()?;
        }
        Ok(state)
    }

    /// Write in-memory state back to mmap (no fsync; unified with bg flush).
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

    /// Flush mmap to disk (MS_SYNC); called by background flush task.
    pub fn flush(&mut self) -> Result<()> {
        self.mmap.flush()?;
        self.is_flushed = true;
        self.queued_for_flush = false;
        Ok(())
    }

    pub(crate) fn is_flushed(&self) -> bool {
        self.is_flushed
    }

    pub(crate) fn take_flush_enqueue_marker(&mut self) -> bool {
        if !self.is_flushed && !self.queued_for_flush {
            self.queued_for_flush = true;
            true
        } else {
            false
        }
    }

    fn mark_dirty(&mut self) {
        self.is_flushed = false;
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
        self.mark_dirty();
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
        self.mark_dirty();
        Ok(())
    }

    /// Drain the completed delivery-order prefix and update processed_ts.
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
            // processed_ts is the last completed real timestamp/sequence.
            if let Some(entry) = self.pending_entries.get(count - 1) {
                if entry.timestamp > self.processed_ts {
                    self.processed_ts = entry.timestamp;
                }
            }
            self.pending_entries.drain(..count);
            self.mark_dirty();
        }
        count
    }

    /// Find a retryable pending entry, or mark retry-exhausted entries completed.
    pub(crate) fn take_retryable_pending(
        &mut self,
        config: QueueConsumerConfig,
        now: i64,
    ) -> PendingScanResult {
        let mut changed = false;
        let mut timestamp = None;

        for entry in &mut self.pending_entries {
            if entry.status != PENDING_STATUS_UNACKED {
                continue;
            }
            let recovery_expired = entry.start_time == 0;
            let running_expired = config.running_expired_seconds > 0
                && now.saturating_sub(entry.start_time) >= config.running_expired_seconds as i64;
            if !recovery_expired && !running_expired {
                continue;
            }

            if config.max_retry_count > 0 && entry.retry_count >= config.max_retry_count {
                entry.status = PENDING_STATUS_ACKED;
                changed = true;
                continue;
            }

            entry.retry_count = entry.retry_count.saturating_add(1);
            entry.start_time = now;
            timestamp = Some(entry.timestamp);
            changed = true;
            break;
        }

        if self.cleanup_acked() > 0 {
            changed = true;
        }
        if changed {
            self.mark_dirty();
        }

        PendingScanResult { timestamp, changed }
    }

    /// Get the next timestamp to try; sparse gaps are skipped by index lookup.
    pub fn next_poll_ts(&self) -> i64 {
        self.processed_ts.saturating_add(1)
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

    pub(crate) fn mark_unacked_recovery_expired(&mut self) -> bool {
        let mut changed = false;
        for entry in &mut self.pending_entries {
            if entry.status == PENDING_STATUS_UNACKED && entry.start_time != 0 {
                entry.start_time = 0;
                changed = true;
            }
        }
        if changed {
            self.mark_dirty();
        }
        changed
    }

    /// Write in-memory state back to mmap (alias for sync_to_mmap).
    pub fn sync(&mut self) -> Result<()> {
        self.sync_to_mmap()
    }
}

pub(crate) struct PendingScanResult {
    pub(crate) timestamp: Option<i64>,
    pub(crate) changed: bool,
}

#[derive(Clone)]
pub(crate) struct ConsumerGroupState {
    state_file: Arc<Mutex<ConsumerStateFile>>,
    closed: Arc<AtomicBool>,
    callback_slots: Vec<QueuePollCallbackSlot>,
}

impl ConsumerGroupState {
    fn new(state_file: Arc<Mutex<ConsumerStateFile>>) -> Self {
        Self {
            state_file,
            closed: Arc::new(AtomicBool::new(false)),
            callback_slots: Vec::new(),
        }
    }

    fn state_file(&self) -> Arc<Mutex<ConsumerStateFile>> {
        Arc::clone(&self.state_file)
    }

    fn closed(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.closed)
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn mark_closed(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    fn add_callback_slot(&mut self, slot: QueuePollCallbackSlot) {
        self.callback_slots.push(slot);
    }

    fn clear_callback_slots(&mut self) -> Result<()> {
        for slot in &self.callback_slots {
            let mut guard = slot
                .lock()
                .map_err(|_| TmslError::InvalidData("queue callback slot mutex poisoned".into()))?;
            *guard = None;
        }
        self.callback_slots.clear();
        Ok(())
    }
}

fn read_i64_at(mmap: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(mmap[offset..offset + 8].try_into().unwrap())
}

pub(crate) fn now_unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn write_i64_at(mmap: &mut MmapMut, offset: usize, val: i64) {
    mmap[offset..offset + 8].copy_from_slice(&val.to_le_bytes());
}

/// Shared internal state for a dataset queue.
pub(crate) struct QueueInner {
    consumers: HashMap<String, ConsumerGroupState>,
    consumer_configs: HashMap<String, QueueConsumerConfig>,
    closed: Arc<AtomicBool>,
}

impl QueueInner {
    pub(crate) fn new() -> Self {
        QueueInner {
            consumers: HashMap::new(),
            consumer_configs: HashMap::new(),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub(crate) fn mark_closed(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    pub(crate) fn close(&mut self) -> Result<()> {
        self.mark_closed();
        for group in self.consumers.values_mut() {
            group.mark_closed();
            group.clear_callback_slots()?;
            let state_file = group.state_file();
            let mut state = state_file
                .lock()
                .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
            state.mark_unacked_recovery_expired();
            state.sync_to_mmap()?;
            state.flush()?;
        }
        self.consumers.clear();
        self.consumer_configs.clear();
        Ok(())
    }

    pub(crate) fn consumers(&self) -> &HashMap<String, ConsumerGroupState> {
        &self.consumers
    }

    pub(crate) fn consumers_mut(&mut self) -> &mut HashMap<String, ConsumerGroupState> {
        &mut self.consumers
    }

    pub(crate) fn consumer_configs(&self) -> &HashMap<String, QueueConsumerConfig> {
        &self.consumer_configs
    }
}

/// Queue handle for a dataset (Clone-safe, singleton per dataset).
pub struct DatasetQueue {
    pub(crate) dataset: Arc<DataSet>,
    pub(crate) inner: Arc<Mutex<QueueInner>>,
    pub(crate) notify: Arc<QueueNotifier>,
    pub(crate) allow_push: bool,
}

impl Clone for DatasetQueue {
    fn clone(&self) -> Self {
        DatasetQueue {
            dataset: Arc::clone(&self.dataset),
            inner: Arc::clone(&self.inner),
            notify: Arc::clone(&self.notify),
            allow_push: self.allow_push,
        }
    }
}

impl fmt::Debug for DatasetQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatasetQueue").finish_non_exhaustive()
    }
}

impl DatasetQueue {
    /// Construct a new DatasetQueue from raw components.
    ///
    /// Used by FFI and Python wrappers that manage dataset lifecycle
    /// separately from Store's internal handle registry.
    pub(crate) fn new(
        dataset: Arc<DataSet>,
        inner: Arc<Mutex<QueueInner>>,
        notify: Arc<QueueNotifier>,
    ) -> Self {
        DatasetQueue {
            dataset,
            inner,
            notify,
            allow_push: true,
        }
    }

    pub(crate) fn new_readonly_producer(
        dataset: Arc<DataSet>,
        inner: Arc<Mutex<QueueInner>>,
        notify: Arc<QueueNotifier>,
    ) -> Self {
        DatasetQueue {
            dataset,
            inner,
            notify,
            allow_push: false,
        }
    }

    /// Open or create a consumer group and return a consumer handle.
    pub fn open_consumer(&self, group_name: &str) -> Result<DatasetQueueConsumer> {
        self.open_consumer_with_config(group_name, QueueConsumerConfig::default())
    }

    /// Open or create a consumer group with explicit retry configuration.
    pub fn open_consumer_with_config(
        &self,
        group_name: &str,
        config: QueueConsumerConfig,
    ) -> Result<DatasetQueueConsumer> {
        validate_consumer_group_name(group_name)?;
        let callback_slot = Arc::new(Mutex::new(None));
        self.notify.register_callback_slot(&callback_slot)?;

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?;

        if inner.is_closed() {
            return Err(TmslError::QueueClosed("queue is closed".into()));
        }
        if let Some(existing) = inner.consumer_configs.get(group_name) {
            if *existing != config {
                return Err(TmslError::InvalidData(format!(
                    "consumer group {group_name} is already open with a different config"
                )));
            }
        }

        let queue_dir = self.dataset.queue_dir()?;

        let (state_file, group_closed) = if let Some(existing) = inner.consumers.get_mut(group_name)
        {
            if existing.is_closed() {
                return Err(TmslError::QueueClosed(format!(
                    "consumer group {group_name} is closed"
                )));
            }
            existing.add_callback_slot(Arc::clone(&callback_slot));
            (existing.state_file(), existing.closed())
        } else {
            let state_path = queue_dir.join(group_name);
            // Determine initial processed_ts from the dataset.
            // Empty datasets start before the signed timestamp domain we poll.
            let initial_ts = self.dataset.latest_written_timestamp().unwrap_or(i64::MIN);
            let sf = Arc::new(Mutex::new(ConsumerStateFile::open_or_create(
                state_path, initial_ts,
            )?));
            let mut group = ConsumerGroupState::new(Arc::clone(&sf));
            let group_closed = group.closed();
            group.add_callback_slot(Arc::clone(&callback_slot));
            inner.consumers.insert(group_name.to_string(), group);
            inner
                .consumer_configs
                .insert(group_name.to_string(), config);
            (sf, group_closed)
        };

        let consumer = DatasetQueueConsumer {
            group_name: group_name.to_string(),
            state_file,
            config,
            notify: Arc::clone(&self.notify),
            dataset: Arc::clone(&self.dataset),
            closed: Arc::clone(&inner.closed),
            group_closed,
            queue_inner: Arc::clone(&self.inner),
            poll_callback: callback_slot,
        };
        drop(inner);
        consumer.enqueue_state_flush()?;
        Ok(consumer)
    }

    /// List created consumer-group state file names in the queue directory.
    ///
    /// This is a directory listing only: entries are not opened or validated as
    /// QSTF files.
    pub fn get_consumer_group_names(&self) -> Result<Vec<String>> {
        let queue_dir = self.dataset.queue_dir()?;
        let read_dir = match fs::read_dir(queue_dir) {
            Ok(read_dir) => read_dir,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        };
        let mut names = Vec::new();
        for entry in read_dir {
            let entry = entry?;
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        names.sort();
        Ok(names)
    }

    /// Drop (close and remove) a consumer group.
    pub fn drop_consumer(&self, group_name: &str) -> Result<()> {
        validate_consumer_group_name(group_name)?;
        let queue_dir = self.dataset.queue_dir()?;
        let state_path = queue_dir.join(group_name);

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?;

        if inner.is_closed() {
            return Err(TmslError::QueueClosed("queue is closed".into()));
        }

        if let Some(group) = inner.consumers_mut().get_mut(group_name) {
            group.mark_closed();
            group.clear_callback_slots()?;
            let state_file = group.state_file();
            let mut guard = state_file
                .lock()
                .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
            guard.mark_unacked_recovery_expired();
            guard.sync()?;
            guard.flush()?;
        }

        let removed = inner.consumers_mut().remove(group_name);
        inner.consumer_configs.remove(group_name);
        if removed.is_none() && !state_path.exists() {
            return Err(TmslError::ConsumerGroupNotFound(group_name.to_string()));
        }

        match fs::remove_file(&state_path) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }

        Ok(())
    }

    /// Push data into the dataset queue.
    ///
    /// Assigns an auto-increment timestamp (latest_written_timestamp + 1),
    /// writes to the dataset, and notifies waiting consumers.
    pub fn push(&self, data: &[u8]) -> Result<i64> {
        if !self.allow_push {
            return Err(TmslError::InvalidData(
                "queue producer is read-only for this dataset".into(),
            ));
        }
        if self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?
            .is_closed()
        {
            return Err(TmslError::QueueClosed("queue is closed".into()));
        }

        let timestamp = self.dataset.write_next_queue_record(data)?;

        // Dataset write already emits the data callback; keep the legacy waiter wake.
        self.notify.notify_waiters()?;

        Ok(timestamp)
    }

    /// Close the queue (marks as closed, drops all consumers).
    pub fn close(&self) -> Result<()> {
        self.dataset.close_queue()?;

        // Close wakes blocking polls, but is not a data notification callback.
        self.notify.notify_waiters()?;

        Ok(())
    }
}

/// Consumer handle for a specific consumer group.
///
/// Multiple consumers can exist for the same group, sharing progress
/// via the shared state file. poll() is exclusive via state file mutex.
pub struct DatasetQueueConsumer {
    group_name: String,
    state_file: Arc<Mutex<ConsumerStateFile>>,
    config: QueueConsumerConfig,
    notify: Arc<QueueNotifier>,
    dataset: Arc<DataSet>,
    closed: Arc<AtomicBool>,
    group_closed: Arc<AtomicBool>,
    queue_inner: Arc<Mutex<QueueInner>>,
    poll_callback: QueuePollCallbackSlot,
}

impl Clone for DatasetQueueConsumer {
    fn clone(&self) -> Self {
        DatasetQueueConsumer {
            group_name: self.group_name.clone(),
            state_file: Arc::clone(&self.state_file),
            config: self.config,
            notify: Arc::clone(&self.notify),
            dataset: Arc::clone(&self.dataset),
            closed: Arc::clone(&self.closed),
            group_closed: Arc::clone(&self.group_closed),
            queue_inner: Arc::clone(&self.queue_inner),
            poll_callback: Arc::clone(&self.poll_callback),
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
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst) || self.group_closed.load(Ordering::SeqCst)
    }

    fn ensure_open(&self) -> Result<()> {
        if self.is_closed() {
            Err(TmslError::QueueClosed("queue consumer is closed".into()))
        } else {
            Ok(())
        }
    }

    /// Flush the current consumer-group state file.
    pub fn flush(&self) -> Result<()> {
        self.ensure_open()?;
        let mut sf = self
            .state_file
            .lock()
            .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
        sf.sync_to_mmap()?;
        sf.flush()
    }

    /// Close this consumer group.
    pub fn close(&self) -> Result<()> {
        if self.group_closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        {
            let mut sf = self
                .state_file
                .lock()
                .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
            sf.mark_unacked_recovery_expired();
            sf.sync_to_mmap()?;
            sf.flush()?;
        }

        let mut inner = self
            .queue_inner
            .lock()
            .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?;
        let remove_group = inner
            .consumers()
            .get(&self.group_name)
            .map(|group| Arc::ptr_eq(&group.closed(), &self.group_closed))
            .unwrap_or(false);
        if remove_group {
            if let Some(mut group) = inner.consumers_mut().remove(&self.group_name) {
                group.clear_callback_slots()?;
            }
            inner.consumer_configs.remove(&self.group_name);
        }
        drop(inner);

        self.notify.notify_waiters()?;
        Ok(())
    }

    /// Inspect this consumer-group state.
    pub fn inspect(&self) -> Result<DatasetQueueConsumerInspectResult> {
        self.ensure_open()?;
        let sf = self
            .state_file
            .lock()
            .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
        let pending_entries = sf
            .pending_entries()
            .iter()
            .map(|entry| DatasetQueueConsumerPendingEntry {
                timestamp: entry.timestamp,
                start_time: entry.start_time,
                status: entry.status,
                retry_count: entry.retry_count,
            })
            .collect();
        Ok(DatasetQueueConsumerInspectResult {
            info: DatasetQueueConsumerInfo {
                group_name: self.group_name.clone(),
                running_expired_seconds: self.config.running_expired_seconds,
                max_retry_count: self.config.max_retry_count,
            },
            state: DatasetQueueConsumerState {
                processed_ts: sf.processed_ts(),
                pending_entries,
            },
        })
    }

    /// Poll for the next unacked record.
    ///
    /// - If a pending entry is expired, retry or discard it according to config.
    /// - Otherwise, read from the dataset after the last completed real entry.
    /// - If no data is available, wait up to `timeout` for a push notification.
    ///
    /// Returns `Ok(Some((timestamp, data)))` if a record is found,
    /// `Ok(None)` if timeout expires, or `Err(QueueClosed)` if the queue was closed.
    pub fn poll(&self, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>> {
        self.ensure_open()?;

        if let Some(row) = self.try_poll_available()? {
            Ok(Some(row))
        } else {
            self.wait_for_data(timeout)
        }
    }

    fn try_poll_available(&self) -> Result<Option<(i64, Vec<u8>)>> {
        self.ensure_open()?;
        let now = now_unix_seconds();
        let retry_scan = {
            let mut sf = self
                .state_file
                .lock()
                .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
            sf.take_retryable_pending(self.config, now)
        };
        if retry_scan.changed {
            self.enqueue_state_flush()?;
        }
        if let Some(ts) = retry_scan.timestamp {
            return self.read_record_at(ts);
        }

        let mut sf = self
            .state_file
            .lock()
            .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
        let next_ts = sf.next_poll_ts();
        let direct = if sf.is_in_pending(next_ts) {
            None
        } else {
            self.dataset.read(next_ts)?
        };
        let row = if direct.is_some() {
            direct
        } else {
            let entries = self.dataset.query_index_entries(next_ts, i64::MAX)?;
            let mut found = None;
            for entry in entries {
                if entry.is_filler() || sf.is_in_pending(entry.timestamp) {
                    continue;
                }
                found = Some(self.dataset.read_entry_at_index(&entry)?);
                break;
            }
            found
        };

        if let Some((ts, data)) = row {
            if !sf.is_in_pending(ts) {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                sf.add_pending(PendingEntry {
                    timestamp: ts,
                    start_time: now,
                    status: PENDING_STATUS_UNACKED,
                    retry_count: 0,
                })?;
                let should_queue = sf.take_flush_enqueue_marker();
                drop(sf);
                if should_queue {
                    self.enqueue_state_flush_target()?;
                }
                return Ok(Some((ts, data)));
            }
            Ok(Some((ts, data)))
        } else {
            Ok(None)
        }
    }

    fn wait_for_data(&self, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>> {
        let (lock, cvar) = self.notify.wait_parts();
        let mut notified = lock
            .lock()
            .map_err(|_| TmslError::InvalidData("notify mutex poisoned".into()))?;

        let deadline = Instant::now() + timeout;

        loop {
            self.ensure_open()?;

            if *notified {
                *notified = false;
                drop(notified);

                if let Some(row) = self.try_poll_available()? {
                    return Ok(Some(row));
                } else {
                    notified = lock
                        .lock()
                        .map_err(|_| TmslError::InvalidData("notify mutex poisoned".into()))?;
                    continue;
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
                return self.try_poll_available();
            }
        }
    }

    fn read_record_at(&self, timestamp: i64) -> Result<Option<(i64, Vec<u8>)>> {
        self.dataset.read(timestamp)
    }

    fn enqueue_state_flush(&self) -> Result<()> {
        let mut sf = self
            .state_file
            .lock()
            .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
        if sf.take_flush_enqueue_marker() {
            drop(sf);
            self.enqueue_state_flush_target()?;
        }
        Ok(())
    }

    fn enqueue_state_flush_target(&self) -> Result<()> {
        self.dataset.enqueue_queue_state_flush(&self.group_name)
    }

    /// Ack a previously polled record.
    pub fn ack(&self, timestamp: i64) -> Result<()> {
        self.ensure_open()?;

        let mut sf = self
            .state_file
            .lock()
            .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;

        sf.ack_pending(timestamp)?;
        sf.cleanup_acked();
        let should_queue = sf.take_flush_enqueue_marker();
        drop(sf);
        if should_queue {
            self.enqueue_state_flush_target()?;
        }
        Ok(())
    }

    /// Register or clear a lightweight wake callback for queue data notifications.
    ///
    /// The callback is invoked synchronously after queue waiters are notified.
    /// It is best-effort and must only wake external processing; it must not
    /// perform data processing or rely on exact notification counts.
    pub fn poll_callback(&self, callback: Option<QueuePollCallback>) -> Result<()> {
        self.ensure_open()?;
        let mut slot = self
            .poll_callback
            .lock()
            .map_err(|_| TmslError::InvalidData("queue callback slot mutex poisoned".into()))?;
        if callback.is_some() && slot.is_some() {
            return Err(TmslError::InvalidData(
                "queue poll callback is already registered".into(),
            ));
        }
        *slot = callback;
        Ok(())
    }
}

/// Queue directory path for a dataset.
pub(crate) fn queue_dir_for(base_dir: &Path) -> PathBuf {
    base_dir.join("queue")
}

/// Flush all consumer state files for a dataset (called by background task).
pub(crate) fn flush_queue_state_files(inner: &Arc<Mutex<QueueInner>>) -> Result<()> {
    let guard = inner
        .lock()
        .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?;

    for group in guard.consumers.values() {
        let sf = group.state_file();
        if let Ok(mut state) = sf.lock() {
            state.sync_to_mmap()?;
            state.flush()?;
        };
    }
    Ok(())
}

/// Flush one consumer state file for a dataset.
pub(crate) fn flush_queue_state_file(
    inner: &Arc<Mutex<QueueInner>>,
    group_name: &str,
) -> Result<()> {
    let state_file = {
        let guard = inner
            .lock()
            .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?;
        guard
            .consumers
            .get(group_name)
            .map(|group| group.state_file())
    };
    if let Some(sf) = state_file {
        let mut state = sf
            .lock()
            .map_err(|_| TmslError::InvalidData("consumer state mutex poisoned".into()))?;
        state.sync_to_mmap()?;
        state.flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_queue_dir() -> PathBuf {
        let d = std::env::temp_dir().join("timslite_queue_tests");
        fs::create_dir_all(&d).unwrap();
        d.join(format!(
            "test_{:?}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn cleanup(dir: &Path) {
        let _ = fs::remove_dir_all(dir);
    }

    fn write_raw_state_file(path: &Path, processed_ts: i64, entries: &[PendingEntry]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();
        file.set_len(STATE_FILE_SIZE as u64).unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        mmap[0..4].copy_from_slice(QUEUE_STATE_MAGIC);
        mmap[4..8].copy_from_slice(&QUEUE_STATE_VERSION.to_le_bytes());
        mmap[8..10].copy_from_slice(&8u16.to_le_bytes());
        write_i64_at(&mut mmap, 10, processed_ts);
        mmap[18..20].copy_from_slice(&(entries.len() as u16).to_le_bytes());
        mmap[20] = PENDING_ENTRY_SIZE as u8;
        for (i, entry) in entries.iter().enumerate() {
            let offset = STATE_HEADER_SIZE + i * PENDING_ENTRY_SIZE;
            let mut buf = [0u8; PENDING_ENTRY_SIZE];
            entry.serialize_to(&mut buf);
            mmap[offset..offset + PENDING_ENTRY_SIZE].copy_from_slice(&buf);
        }
        mmap.flush().unwrap();
    }

    #[test]
    fn pending_entry_round_trip() {
        assert_eq!(QUEUE_STATE_VERSION, 1);
        assert_eq!(PENDING_ENTRY_SIZE, 18);
        assert_eq!(MAX_PENDING_ENTRIES, 226);
        let entry = PendingEntry {
            timestamp: 1700000000,
            start_time: 1700000005,
            status: PENDING_STATUS_UNACKED,
            retry_count: 2,
        };
        let mut buf = [0u8; PENDING_ENTRY_SIZE];
        entry.serialize_to(&mut buf);
        let restored = PendingEntry::deserialize_from(&buf);
        assert_eq!(restored.timestamp, entry.timestamp);
        assert_eq!(restored.start_time, entry.start_time);
        assert_eq!(restored.status, entry.status);
        assert_eq!(restored.retry_count, entry.retry_count);
    }

    #[test]
    fn queue_consumer_config_builder_validates_bounds() {
        let default = QueueConsumerConfig::default();
        assert_eq!(default.running_expired_seconds, 900);
        assert_eq!(default.max_retry_count, 3);

        let max = QueueConsumerConfig::builder()
            .running_expired_seconds(u16::MAX as u64)
            .max_retry_count(u8::MAX as u16)
            .build()
            .unwrap();
        assert_eq!(max.running_expired_seconds, u16::MAX);
        assert_eq!(max.max_retry_count, u8::MAX);

        assert!(QueueConsumerConfig::builder()
            .running_expired_seconds(u16::MAX as u64 + 1)
            .build()
            .is_err());
        assert!(QueueConsumerConfig::builder()
            .max_retry_count(u8::MAX as u16 + 1)
            .build()
            .is_err());
    }

    #[test]
    fn pending_entry_acked_status() {
        let entry = PendingEntry {
            timestamp: -42,
            start_time: 0,
            status: PENDING_STATUS_ACKED,
            retry_count: 0,
        };
        let mut buf = [0u8; PENDING_ENTRY_SIZE];
        entry.serialize_to(&mut buf);
        let restored = PendingEntry::deserialize_from(&buf);
        assert_eq!(restored.timestamp, -42);
        assert_eq!(restored.status, PENDING_STATUS_ACKED);
    }

    #[test]
    fn csf_reopen_marks_unacked_pending_recovery_expired_and_keeps_retry_count() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        {
            let mut sf = ConsumerStateFile::open_or_create(path.clone(), 10).unwrap();
            sf.add_pending(PendingEntry {
                timestamp: 11,
                start_time: 12345,
                status: PENDING_STATUS_UNACKED,
                retry_count: 2,
            })
            .unwrap();
            sf.sync_to_mmap().unwrap();
            sf.flush().unwrap();
        }

        let sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        assert_eq!(sf.pending_count(), 1);
        let pending = &sf.pending_entries()[0];
        assert_eq!(pending.timestamp, 11);
        assert_eq!(pending.start_time, 0);
        assert_eq!(pending.retry_count, 2);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_create_and_reopen() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let sf = ConsumerStateFile::open_or_create(path.clone(), 42).unwrap();
        assert_eq!(sf.processed_ts(), 42);
        assert_eq!(sf.pending_count(), 0);
        drop(sf);

        let sf2 = ConsumerStateFile::open_or_create(path, 0).unwrap();
        assert_eq!(sf2.processed_ts(), 42);
        assert_eq!(sf2.pending_count(), 0);
        drop(sf2);
        cleanup(&dir);
    }

    #[test]
    fn csf_dirty_marker_queues_once_until_flush() {
        let dir = temp_queue_dir();
        let path = dir.join("group_marker");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();

        assert!(sf.is_flushed());
        assert!(!sf.take_flush_enqueue_marker());

        sf.add_pending(PendingEntry {
            timestamp: 1,
            start_time: 10,
            status: PENDING_STATUS_UNACKED,
            retry_count: 0,
        })
        .unwrap();

        assert!(!sf.is_flushed());
        assert!(sf.take_flush_enqueue_marker());
        assert!(!sf.take_flush_enqueue_marker());

        sf.sync_to_mmap().unwrap();
        sf.flush().unwrap();

        assert!(sf.is_flushed());
        assert!(!sf.take_flush_enqueue_marker());
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_create_parent_dirs() {
        let dir = temp_queue_dir();
        let path = dir.join("sub").join("deep").join("group1");
        let sf = ConsumerStateFile::open_or_create(path.clone(), 0).unwrap();
        assert!(path.exists());
        assert_eq!(sf.processed_ts(), 0);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_file_size() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let sf = ConsumerStateFile::open_or_create(path.clone(), 0).unwrap();
        drop(sf);
        let meta = fs::metadata(&path).unwrap();
        assert_eq!(meta.len() as usize, STATE_FILE_SIZE);
        cleanup(&dir);
    }

    #[test]
    fn csf_open_existing_with_pending() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        {
            let mut sf = ConsumerStateFile::open_or_create(path.clone(), 100).unwrap();
            sf.add_pending(PendingEntry {
                timestamp: 101,
                start_time: 1000,
                status: PENDING_STATUS_UNACKED,
                retry_count: 0,
            })
            .unwrap();
            sf.add_pending(PendingEntry {
                timestamp: 102,
                start_time: 1001,
                status: PENDING_STATUS_ACKED,
                retry_count: 0,
            })
            .unwrap();
            sf.sync_to_mmap().unwrap();
            sf.flush().unwrap();
        }
        let sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        assert_eq!(sf.processed_ts(), 100);
        assert_eq!(sf.pending_count(), 2);
        assert!(sf.find_pending(101).is_some());
        assert!(sf.find_pending(102).is_some());
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_open_invalid_magic() {
        let dir = temp_queue_dir();
        let path = dir.join("bad");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            file.set_len(STATE_FILE_SIZE as u64).unwrap();
            let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
            mmap[0..4].copy_from_slice(b"BAD!");
            mmap.flush().unwrap();
        }
        let result = ConsumerStateFile::open_or_create(path, 0);
        assert!(matches!(result, Err(TmslError::InvalidMagic)));
        cleanup(&dir);
    }

    #[test]
    fn csf_open_invalid_version() {
        let dir = temp_queue_dir();
        let path = dir.join("bad");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            file.set_len(STATE_FILE_SIZE as u64).unwrap();
            let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
            mmap[0..4].copy_from_slice(QUEUE_STATE_MAGIC);
            mmap[4..8].copy_from_slice(&99u32.to_le_bytes());
            mmap.flush().unwrap();
        }
        let result = ConsumerStateFile::open_or_create(path, 0);
        assert!(matches!(result, Err(TmslError::InvalidVersion(v)) if v == 99));
        cleanup(&dir);
    }

    #[test]
    fn csf_open_rejects_invalid_pending_status() {
        let dir = temp_queue_dir();
        let path = dir.join("bad_status");
        write_raw_state_file(
            &path,
            10,
            &[PendingEntry {
                timestamp: 11,
                start_time: 1000,
                status: 9,
                retry_count: 0,
            }],
        );
        let result = ConsumerStateFile::open_or_create(path, 0);
        assert!(
            matches!(result, Err(TmslError::InvalidData(msg)) if msg.contains("pending status"))
        );
        cleanup(&dir);
    }

    #[test]
    fn csf_open_rejects_duplicate_or_unsorted_pending_timestamps() {
        let dir = temp_queue_dir();
        let duplicate = dir.join("duplicate");
        write_raw_state_file(
            &duplicate,
            10,
            &[
                PendingEntry {
                    timestamp: 11,
                    start_time: 1000,
                    status: PENDING_STATUS_UNACKED,
                    retry_count: 0,
                },
                PendingEntry {
                    timestamp: 11,
                    start_time: 1001,
                    status: PENDING_STATUS_ACKED,
                    retry_count: 0,
                },
            ],
        );
        let duplicate_result = ConsumerStateFile::open_or_create(duplicate, 0);
        assert!(
            matches!(duplicate_result, Err(TmslError::InvalidData(msg)) if msg.contains("pending timestamp order"))
        );

        let unsorted = dir.join("unsorted");
        write_raw_state_file(
            &unsorted,
            10,
            &[
                PendingEntry {
                    timestamp: 12,
                    start_time: 1000,
                    status: PENDING_STATUS_UNACKED,
                    retry_count: 0,
                },
                PendingEntry {
                    timestamp: 11,
                    start_time: 1001,
                    status: PENDING_STATUS_UNACKED,
                    retry_count: 0,
                },
            ],
        );
        let unsorted_result = ConsumerStateFile::open_or_create(unsorted, 0);
        assert!(
            matches!(unsorted_result, Err(TmslError::InvalidData(msg)) if msg.contains("pending timestamp order"))
        );
        cleanup(&dir);
    }

    #[test]
    fn csf_add_pending_increments_count() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        assert_eq!(sf.pending_count(), 0);
        sf.add_pending(PendingEntry {
            timestamp: 1,
            start_time: 0,
            status: PENDING_STATUS_UNACKED,
            retry_count: 0,
        })
        .unwrap();
        assert_eq!(sf.pending_count(), 1);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_add_pending_full() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        for i in 0..MAX_PENDING_ENTRIES {
            sf.add_pending(PendingEntry {
                timestamp: i as i64,
                start_time: 0,
                status: PENDING_STATUS_UNACKED,
                retry_count: 0,
            })
            .unwrap();
        }
        let result = sf.add_pending(PendingEntry {
            timestamp: 9999,
            start_time: 0,
            status: PENDING_STATUS_UNACKED,
            retry_count: 0,
        });
        assert!(matches!(result, Err(TmslError::PendingFull(_))));
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_ack_pending_marks_acked() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 42,
            start_time: 0,
            status: PENDING_STATUS_UNACKED,
            retry_count: 0,
        })
        .unwrap();
        sf.ack_pending(42).unwrap();
        assert!(sf.find_pending(42).unwrap().status == PENDING_STATUS_ACKED);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_ack_nonexistent_errors() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        let result = sf.ack_pending(999);
        assert!(result.is_err());
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_cleanup_acked_consecutive() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 10,
            start_time: 0,
            status: PENDING_STATUS_ACKED,
            retry_count: 0,
        })
        .unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 11,
            start_time: 0,
            status: PENDING_STATUS_ACKED,
            retry_count: 0,
        })
        .unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 12,
            start_time: 0,
            status: PENDING_STATUS_UNACKED,
            retry_count: 0,
        })
        .unwrap();
        let cleaned = sf.cleanup_acked();
        assert_eq!(cleaned, 2);
        assert_eq!(sf.pending_count(), 1);
        assert_eq!(sf.processed_ts(), 11);
        assert_eq!(sf.pending_entries()[0].timestamp, 12);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_cleanup_acked_empty() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        assert_eq!(sf.cleanup_acked(), 0);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_cleanup_acked_persists() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        {
            let mut sf = ConsumerStateFile::open_or_create(path.clone(), 10).unwrap();
            sf.add_pending(PendingEntry {
                timestamp: 11,
                start_time: 0,
                status: PENDING_STATUS_UNACKED,
                retry_count: 0,
            })
            .unwrap();
            sf.ack_pending(11).unwrap();
            sf.cleanup_acked();
            sf.sync_to_mmap().unwrap();
            sf.flush().unwrap();
        }
        let sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        assert_eq!(sf.processed_ts(), 11);
        assert_eq!(sf.pending_count(), 0);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_take_retryable_pending_skips_unexpired() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 100,
            start_time: 1000,
            status: PENDING_STATUS_UNACKED,
            retry_count: 0,
        })
        .unwrap();
        let config = QueueConsumerConfig::builder()
            .running_expired_seconds(60)
            .build()
            .unwrap();
        let scan = sf.take_retryable_pending(config, 1010);
        assert!(scan.timestamp.is_none());
        assert!(!scan.changed);
        assert_eq!(sf.pending_count(), 1);
        assert_eq!(sf.pending_entries()[0].retry_count, 0);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_take_retryable_pending_increments_retry() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 100,
            start_time: 1000,
            status: PENDING_STATUS_UNACKED,
            retry_count: 0,
        })
        .unwrap();
        let config = QueueConsumerConfig::builder()
            .running_expired_seconds(10)
            .max_retry_count(3)
            .build()
            .unwrap();
        let scan = sf.take_retryable_pending(config, 1010);
        assert_eq!(scan.timestamp, Some(100));
        assert!(scan.changed);
        assert_eq!(sf.pending_entries()[0].retry_count, 1);
        assert_eq!(sf.pending_entries()[0].start_time, 1010);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_take_retryable_pending_drops_retry_exhausted_prefix() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 1,
            start_time: 1000,
            status: PENDING_STATUS_UNACKED,
            retry_count: 1,
        })
        .unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 2,
            start_time: 1010,
            status: PENDING_STATUS_UNACKED,
            retry_count: 0,
        })
        .unwrap();
        let config = QueueConsumerConfig::builder()
            .running_expired_seconds(10)
            .max_retry_count(1)
            .build()
            .unwrap();
        let scan = sf.take_retryable_pending(config, 1010);
        assert!(scan.timestamp.is_none());
        assert!(scan.changed);
        assert_eq!(sf.processed_ts(), 1);
        assert_eq!(sf.pending_count(), 1);
        assert_eq!(sf.pending_entries()[0].timestamp, 2);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_next_poll_ts_positive() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let sf = ConsumerStateFile::open_or_create(path, 100).unwrap();
        assert_eq!(sf.next_poll_ts(), 101);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_next_poll_ts_zero() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        assert_eq!(sf.next_poll_ts(), 1);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_sync_persists_data() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        {
            let mut sf = ConsumerStateFile::open_or_create(path.clone(), 0).unwrap();
            sf.add_pending(PendingEntry {
                timestamp: 99,
                start_time: 88,
                status: PENDING_STATUS_UNACKED,
                retry_count: 0,
            })
            .unwrap();
            sf.sync_to_mmap().unwrap();
            sf.flush().unwrap();
        }
        let sf2 = ConsumerStateFile::open_or_create(path, 0).unwrap();
        assert_eq!(sf2.pending_count(), 1);
        assert_eq!(sf2.pending_entries()[0].timestamp, 99);
        assert_eq!(sf2.pending_entries()[0].start_time, 0);
        assert_eq!(sf2.pending_entries()[0].retry_count, 0);
        drop(sf2);
        cleanup(&dir);
    }

    #[test]
    fn csf_is_in_pending_and_find() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 42,
            start_time: 0,
            status: PENDING_STATUS_UNACKED,
            retry_count: 0,
        })
        .unwrap();
        assert!(sf.is_in_pending(42));
        assert!(!sf.is_in_pending(99));
        assert_eq!(sf.find_pending(42).unwrap().timestamp, 42);
        assert!(sf.find_pending(99).is_none());
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn qi_new_empty() {
        let qi = QueueInner::new();
        assert!(!qi.is_closed());
        assert!(qi.consumers().is_empty());
    }

    #[test]
    fn qi_close() {
        let mut qi = QueueInner::new();
        qi.close().unwrap();
        assert!(qi.is_closed());
    }

    #[test]
    fn qi_consumers_can_add() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        let mut qi = QueueInner::new();
        qi.consumers_mut().insert(
            "group1".to_string(),
            ConsumerGroupState::new(Arc::new(Mutex::new(sf))),
        );
        assert_eq!(qi.consumers().len(), 1);
        assert!(qi.consumers().contains_key("group1"));
        drop(qi);
        cleanup(&dir);
    }

    #[test]
    fn qi_multiple_groups() {
        let dir = temp_queue_dir();
        let sf1 = ConsumerStateFile::open_or_create(dir.join("g1"), 0).unwrap();
        let sf2 = ConsumerStateFile::open_or_create(dir.join("g2"), 0).unwrap();
        let mut qi = QueueInner::new();
        qi.consumers_mut().insert(
            "g1".to_string(),
            ConsumerGroupState::new(Arc::new(Mutex::new(sf1))),
        );
        qi.consumers_mut().insert(
            "g2".to_string(),
            ConsumerGroupState::new(Arc::new(Mutex::new(sf2))),
        );
        assert_eq!(qi.consumers().len(), 2);
        qi.close().unwrap();
        assert!(qi.is_closed());
        drop(qi);
        cleanup(&dir);
    }
}
