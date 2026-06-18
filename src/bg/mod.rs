//! Background task executor (flush, idle-check, cache eviction, retention reclaim).
//!
//! Supports two modes:
//! - **Auto (default)**: `BackgroundTasks::start` spawns a dedicated thread.
//! - **Manual**: `BackgroundTasks::new` creates the executor without a thread;
//!   callers drive it via `tick()`.
//!
//! Both modes share a `Mutex<ExecutorState>` for concurrency safety.

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::cache::BlockCache;
use crate::dataset::DataSetKey;
use crate::dataset::{DataSet, DataSetFlushTarget, SegmentFlushQueue, SegmentFlushTarget};
use crate::journal::JournalManager;

type DatasetMap = HashMap<DataSetKey, Arc<DataSet>>;

/// Shared scheduling state kept across tick invocations.
pub struct ExecutorState {
    pub last_flush: Instant,
    pub last_idle_check: Instant,
    pub last_cache_eviction: Instant,
    pub next_retention: Instant,
    pub flush_running: bool,
    pub idle_running: bool,
    pub cache_running: bool,
    pub retention_running: bool,
}

/// Result returned by a single `tick()` call.
#[derive(Clone, Debug)]
pub struct TickResult {
    /// Number of tasks actually executed (0..=4).
    pub executed_tasks: usize,
    /// Duration until the next task becomes due, saturating at 0.
    pub next_delay: Duration,
}

/// Background task manager.
pub struct BackgroundTasks {
    state: Arc<Mutex<ExecutorState>>,
    datasets: Arc<RwLock<DatasetMap>>,
    flush_queue: SegmentFlushQueue,
    journal: Arc<JournalManager>,
    block_cache: Arc<BlockCache>,
    flush_interval: Duration,
    idle_timeout: Duration,
    cache_idle_timeout: Duration,
    retention_check_hour: u8,
    handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

#[derive(Default)]
struct DueTasks {
    flush: bool,
    idle_check: bool,
    cache_eviction: bool,
    retention_reclaim: bool,
}

impl DueTasks {
    fn count(&self) -> usize {
        self.flush as usize
            + self.idle_check as usize
            + self.cache_eviction as usize
            + self.retention_reclaim as usize
    }
}

/// Compute the UTC-based delay until the next retention reclaim target.
fn retention_delay_secs_utc(check_hour: u8, secs_since_epoch: u64) -> u64 {
    let hour = (check_hour as u64) % 24;
    let secs_into_day = secs_since_epoch % 86400;
    let target_secs_into_day = hour * 3600;
    let wait_secs = if target_secs_into_day > secs_into_day {
        target_secs_into_day - secs_into_day
    } else {
        // Already past today's target; schedule for tomorrow.
        86400 - (secs_into_day - target_secs_into_day)
    };
    wait_secs.max(1)
}

/// Compute the next wall-clock Instant at which retention reclaim should run.
/// `check_hour` is 0-23 representing the UTC hour of day.
fn next_retention_time(check_hour: u8) -> Instant {
    let now = Instant::now();
    let secs_since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    now + Duration::from_secs(retention_delay_secs_utc(check_hour, secs_since_epoch))
}

/// Idle-check interval (fixed).
const IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(60);
/// Cache-eviction interval (fixed).
const CACHE_EVICTION_INTERVAL: Duration = Duration::from_secs(60);

impl BackgroundTasks {
    /// Create the executor without spawning a background thread.
    pub fn new(
        datasets: Arc<RwLock<DatasetMap>>,
        flush_queue: SegmentFlushQueue,
        journal: Arc<JournalManager>,
        block_cache: Arc<BlockCache>,
        flush_interval: Duration,
        idle_timeout: Duration,
        cache_idle_timeout: Duration,
        retention_check_hour: u8,
    ) -> Self {
        let now = Instant::now();
        Self {
            state: Arc::new(Mutex::new(ExecutorState {
                last_flush: now,
                last_idle_check: now,
                last_cache_eviction: now,
                next_retention: next_retention_time(retention_check_hour),
                flush_running: false,
                idle_running: false,
                cache_running: false,
                retention_running: false,
            })),
            datasets,
            flush_queue,
            journal,
            block_cache,
            flush_interval,
            idle_timeout,
            cache_idle_timeout,
            retention_check_hour,
            handle: None,
            shutdown_tx: None,
        }
    }

    /// Create the executor AND spawn a background thread.
    pub fn start(
        datasets: Arc<RwLock<DatasetMap>>,
        flush_queue: SegmentFlushQueue,
        journal: Arc<JournalManager>,
        block_cache: Arc<BlockCache>,
        flush_interval: Duration,
        idle_timeout: Duration,
        cache_idle_timeout: Duration,
        retention_check_hour: u8,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = mpsc::channel();

        let now = Instant::now();
        let state = Arc::new(Mutex::new(ExecutorState {
            last_flush: now,
            last_idle_check: now,
            last_cache_eviction: now,
            next_retention: next_retention_time(retention_check_hour),
            flush_running: false,
            idle_running: false,
            cache_running: false,
            retention_running: false,
        }));

        // Clone Arcs for the thread
        let thread_state = Arc::clone(&state);
        let thread_datasets = Arc::clone(&datasets);
        let thread_flush_queue = Arc::clone(&flush_queue);
        let thread_journal = Arc::clone(&journal);
        let thread_block_cache = Arc::clone(&block_cache);

        let handle = std::thread::spawn(move || {
            let bg = BackgroundTasks {
                state: thread_state,
                datasets: thread_datasets,
                flush_queue: thread_flush_queue,
                journal: thread_journal,
                block_cache: thread_block_cache,
                flush_interval,
                idle_timeout,
                cache_idle_timeout,
                retention_check_hour,
                handle: None,
                shutdown_tx: None,
            };
            bg.thread_loop(shutdown_rx);
        });

        Self {
            state,
            datasets,
            flush_queue,
            journal,
            block_cache,
            flush_interval,
            idle_timeout,
            cache_idle_timeout,
            retention_check_hour,
            handle: Some(handle),
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Execute a single tick: check which tasks are due, run them, return
    /// how many tasks were executed plus the delay until the next is due.
    pub fn tick(&self) -> TickResult {
        let (due_tasks, next_delay) = self.reserve_due_tasks();
        let executed = due_tasks.count();
        self.execute_reserved_tasks(&due_tasks);
        self.finish_reserved_tasks(&due_tasks);
        TickResult {
            executed_tasks: executed,
            next_delay,
        }
    }

    /// Return the duration until the next task is due, without executing.
    pub fn next_delay(&self) -> Duration {
        let state = self.state.lock().unwrap();
        self.compute_next_delay(&state)
    }

    pub(crate) fn flush_queue(&self) -> &SegmentFlushQueue {
        &self.flush_queue
    }

    /// Stop the background thread (no-op if no thread was started).
    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }

    fn compute_next_delay(&self, state: &ExecutorState) -> Duration {
        let now = Instant::now();
        let next_flush = if state.flush_running {
            now + self.flush_interval
        } else {
            state.last_flush + self.flush_interval
        };
        let next_idle = if state.idle_running {
            now + IDLE_CHECK_INTERVAL
        } else {
            state.last_idle_check + IDLE_CHECK_INTERVAL
        };
        let next_retention = if state.retention_running {
            now + Duration::from_secs(1)
        } else {
            state.next_retention
        };
        let mut next = next_flush.min(next_idle).min(next_retention);
        if self.block_cache.is_enabled() {
            let next_cache = if state.cache_running {
                now + CACHE_EVICTION_INTERVAL
            } else {
                state.last_cache_eviction + CACHE_EVICTION_INTERVAL
            };
            next = next.min(next_cache);
        }
        next.saturating_duration_since(now)
    }

    fn reserve_due_tasks(&self) -> (DueTasks, Duration) {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        let mut due = DueTasks::default();

        if now >= state.last_flush + self.flush_interval && !state.flush_running {
            state.last_flush = now;
            state.flush_running = true;
            due.flush = true;
        }

        if now >= state.last_idle_check + IDLE_CHECK_INTERVAL && !state.idle_running {
            state.last_idle_check = now;
            state.idle_running = true;
            due.idle_check = true;
        }

        if self.block_cache.is_enabled()
            && now >= state.last_cache_eviction + CACHE_EVICTION_INTERVAL
            && !state.cache_running
        {
            state.last_cache_eviction = now;
            state.cache_running = true;
            due.cache_eviction = true;
        }

        if now >= state.next_retention && !state.retention_running {
            state.next_retention = next_retention_time(self.retention_check_hour);
            state.retention_running = true;
            due.retention_reclaim = true;
        }

        let next_delay = self.compute_next_delay(&state);
        (due, next_delay)
    }

    fn execute_reserved_tasks(&self, due: &DueTasks) {
        if due.flush {
            self.run_flush();
        }
        if due.idle_check {
            self.run_idle_check();
        }
        if due.cache_eviction {
            self.run_cache_eviction();
        }
        if due.retention_reclaim {
            self.run_retention_reclaim();
        }
    }

    fn finish_reserved_tasks(&self, due: &DueTasks) {
        if due.count() == 0 {
            return;
        }
        let mut state = self.state.lock().unwrap();
        if due.flush {
            state.flush_running = false;
        }
        if due.idle_check {
            state.idle_running = false;
        }
        if due.cache_eviction {
            state.cache_running = false;
        }
        if due.retention_reclaim {
            state.retention_running = false;
        }
    }

    fn run_flush(&self) {
        let grouped_targets = self.drain_flush_targets_by_dataset();
        for (key, targets) in grouped_targets {
            if JournalManager::is_journal_key(&key) {
                continue;
            }
            let ds_arc = {
                match self.datasets.read() {
                    Ok(guard) => guard.get(&key).cloned(),
                    Err(_) => None,
                }
            };
            let Some(ds_arc) = ds_arc else {
                continue;
            };
            if let Err(e) = ds_arc.sync_queued_flush_targets(targets) {
                log::error!("[bg flush] failed for {:?}: {}", key, e);
            }
        }
        if let Err(e) = self.journal.flush_dirty() {
            log::error!("[bg flush] failed for journal: {}", e);
        }
    }

    fn drain_flush_targets_by_dataset(&self) -> Vec<(DataSetKey, Vec<SegmentFlushTarget>)> {
        let targets: Vec<DataSetFlushTarget> = {
            let mut queue = self.flush_queue.lock().unwrap();
            queue.drain(..).collect()
        };
        let mut grouped = HashMap::<DataSetKey, Vec<SegmentFlushTarget>>::new();
        for target in targets {
            grouped
                .entry(target.dataset)
                .or_default()
                .push(target.segment);
        }
        grouped.into_iter().collect()
    }

    fn run_idle_check(&self) {
        let idle_keys = {
            let guard = match self.datasets.read() {
                Ok(g) => g,
                Err(_) => return,
            };
            guard
                .iter()
                .filter(|(_k, ds_arc)| ds_arc.last_used_at().elapsed() >= self.idle_timeout)
                .map(|(k, _)| k.clone())
                .collect::<Vec<_>>()
        };

        for key in idle_keys {
            let ds_arc = {
                let guard = match self.datasets.read() {
                    Ok(g) => g,
                    Err(_) => continue,
                };
                match guard.get(&key) {
                    Some(ds) => Arc::clone(ds),
                    None => continue,
                }
            };
            if ds_arc.last_used_at().elapsed() >= self.idle_timeout {
                if let Err(e) = ds_arc.idle_close_segments() {
                    log::error!("[bg idle] close failed for {:?}: {}", key, e);
                } else {
                    log::info!("[bg idle] closed dataset {:?}", key);
                }
            }
        }
    }

    fn run_cache_eviction(&self) {
        if self.block_cache.is_enabled() {
            let evicted = self.block_cache.evict_idle(self.cache_idle_timeout);
            if evicted > 0 {
                log::info!("[bg cache] evicted {} idle entries", evicted);
            }
        }
    }

    fn run_retention_reclaim(&self) {
        let enabled: Vec<(DataSetKey, u64)> = {
            let guard = match self.datasets.read() {
                Ok(g) => g,
                Err(_) => return,
            };
            guard
                .iter()
                .filter_map(|(k, ds_arc)| {
                    let retention_window = ds_arc.retention_window();
                    if retention_window > 0 {
                        Some((k.clone(), retention_window))
                    } else {
                        None
                    }
                })
                .collect()
        };

        let mut total_reclaimed = 0usize;
        for (key, _ret_ms) in enabled {
            let ds_arc = {
                let guard = match self.datasets.read() {
                    Ok(g) => g,
                    Err(_) => continue,
                };
                match guard.get(&key) {
                    Some(ds) => Arc::clone(ds),
                    None => continue,
                }
            };
            match ds_arc.reclaim_expired_segments() {
                Ok(n) if n > 0 => {
                    log::info!("[bg retention] {:?}: reclaimed {} segments", key, n);
                    total_reclaimed += n;
                }
                Err(e) => {
                    log::error!("[bg retention] {:?}: reclaim failed: {}", key, e)
                }
                _ => {}
            }
        }
        if total_reclaimed > 0 {
            log::info!(
                "[bg retention] reclaimed {} segment files total",
                total_reclaimed
            );
        }
    }

    fn thread_loop(&self, shutdown_rx: mpsc::Receiver<()>) {
        loop {
            let wait_time = {
                let state = self.state.lock().unwrap();
                self.compute_next_delay(&state)
            };

            if !wait_time.is_zero() && shutdown_rx.recv_timeout(wait_time).is_ok() {
                log::info!("[bg] received shutdown signal");
                break;
            }

            let result = self.tick();
            if result.executed_tasks == 0 && result.next_delay.is_zero() {
                std::thread::yield_now();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn make_test_dataset(base: &str) -> (DataSetKey, Arc<DataSet>) {
        let dir = std::env::temp_dir().join(base);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let key = DataSetKey {
            name: base.to_string(),
            dataset_type: "test".to_string(),
        };
        let ds = DataSet::create(
            key.clone(),
            dir,
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();
        (key, Arc::new(ds))
    }

    fn make_empty_test_bg(thread_enabled: bool) -> BackgroundTasks {
        let datasets = Arc::new(RwLock::new(HashMap::new()));
        let flush_queue = crate::dataset::DataSetRuntimeContext::new_flush_queue();
        let block_cache = Arc::new(BlockCache::new(0)); // disabled
        let flush_interval = Duration::from_secs(600);
        let idle_timeout = Duration::from_secs(1800);
        let cache_idle_timeout = Duration::from_secs(1800);
        let retention_check_hour = 0u8;

        if thread_enabled {
            BackgroundTasks::start(
                datasets,
                Arc::clone(&flush_queue),
                Arc::new(JournalManager::Disabled),
                block_cache,
                flush_interval,
                idle_timeout,
                cache_idle_timeout,
                retention_check_hour,
            )
        } else {
            BackgroundTasks::new(
                datasets,
                flush_queue,
                Arc::new(JournalManager::Disabled),
                block_cache,
                flush_interval,
                idle_timeout,
                cache_idle_timeout,
                retention_check_hour,
            )
        }
    }

    #[test]
    fn test_next_retention_time_is_not_zero() {
        let nr = next_retention_time(0);
        let diff = nr.saturating_duration_since(Instant::now());
        assert!(diff.as_secs() <= 86400);
    }

    #[test]
    fn test_next_retention_time_clamp_hour() {
        let _ = next_retention_time(25);
        let _ = next_retention_time(23);
    }

    #[test]
    fn test_retention_delay_uses_utc_epoch_day_boundary() {
        assert_eq!(retention_delay_secs_utc(0, 23 * 3600 + 59 * 60 + 59), 1);
        assert_eq!(retention_delay_secs_utc(1, 30 * 60), 30 * 60);
        assert_eq!(retention_delay_secs_utc(23, 24 * 3600 + 22 * 3600), 3600);
        assert_eq!(retention_delay_secs_utc(25, 0), 3600);
    }

    #[test]
    fn test_executor_state_initialized() {
        let bg = make_empty_test_bg(false);
        // Initial delay should be close to flush_interval (shortest interval among tasks)
        let delay = bg.next_delay();
        // Should be approximately <= flush_interval (600s)
        assert!(delay <= Duration::from_secs(600));
    }

    #[test]
    fn test_next_delay_no_side_effects() {
        let bg = make_empty_test_bg(false);
        let d1 = bg.next_delay();
        let d2 = bg.next_delay();
        // Two calls in rapid succession should return nearly identical values
        let diff = d1.abs_diff(d2);
        assert!(diff < Duration::from_millis(100));
    }

    #[test]
    fn test_tick_returns_result_structure() {
        let bg = make_empty_test_bg(false);
        let result = bg.tick();
        // With empty datasets and no expired deadlines (state initialized to now),
        // executed_tasks should be 0 (nothing due)
        assert_eq!(result.executed_tasks, 0);
        // next_delay should be > 0
        assert!(result.next_delay.as_secs_f64() > 0.0);
    }

    #[test]
    fn test_tick_bg_disabled_mode() {
        // Create with no thread; should not panic on tick/next_delay.
        let bg = make_empty_test_bg(false);
        // tick should succeed
        let result = bg.tick();
        assert!(result.next_delay.as_secs_f64() > 0.0);
        // next_delay should succeed
        let _ = bg.next_delay();
    }

    #[test]
    fn test_tick_bg_interval_tasks_due_after_expiry() {
        // Tests the 3 interval-based tasks (flush, idle, cache) separately
        // from retention, which uses wall-clock scheduling and is timing-fragile.
        let datasets = Arc::new(RwLock::new(HashMap::new()));
        let block_cache = Arc::new(BlockCache::new(256 * 1024)); // enabled
        let flush_interval = Duration::from_millis(1);
        let bg = BackgroundTasks::new(
            datasets,
            crate::dataset::DataSetRuntimeContext::new_flush_queue(),
            Arc::new(JournalManager::Disabled),
            block_cache,
            flush_interval,
            Duration::from_secs(1800),
            Duration::from_secs(1800),
            0,
        );

        // Force interval-based deadlines far in the past
        {
            let mut state = bg.state.lock().unwrap();
            state.last_flush = Instant::now() - Duration::from_secs(10);
            state.last_idle_check = Instant::now() - Duration::from_secs(120);
            state.last_cache_eviction = Instant::now() - Duration::from_secs(120);
            // Leave next_retention in the future so only 3 tasks fire
        }

        let result = bg.tick();
        assert_eq!(
            result.executed_tasks, 3,
            "expected flush + idle + cache = 3 tasks"
        );
    }

    #[test]
    fn test_tick_bg_retention_reschedules_after_fire() {
        // Tests retention independently: verify it fires and reschedules
        // to a future wall-clock time, avoiding the timing fragility of
        // combining it with interval-based tasks.
        let datasets = Arc::new(RwLock::new(HashMap::new()));
        let block_cache = Arc::new(BlockCache::new(0));
        let bg = BackgroundTasks::new(
            datasets,
            crate::dataset::DataSetRuntimeContext::new_flush_queue(),
            Arc::new(JournalManager::Disabled),
            block_cache,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            0,
        );

        // Force retention deadline to the past
        {
            let mut state = bg.state.lock().unwrap();
            state.next_retention = Instant::now() - Duration::from_secs(1);
        }

        let result = bg.tick();
        assert!(result.executed_tasks >= 1, "retention task should execute");

        // After firing, next_retention must be rescheduled to the future
        let state = bg.state.lock().unwrap();
        assert!(
            state.next_retention > Instant::now(),
            "next_retention should be rescheduled to a future time"
        );
    }

    #[test]
    fn test_tick_bg_respects_interval() {
        let bg = make_empty_test_bg(false);
        // First tick -> 0 tasks (nothing due)
        let r1 = bg.tick();
        assert_eq!(r1.executed_tasks, 0);
        // Second tick immediately after -> still 0 tasks (interval not passed)
        let r2 = bg.tick();
        assert_eq!(r2.executed_tasks, 0);
    }

    #[test]
    fn test_thread_enabled_external_tick_safe() {
        let mut bg = make_empty_test_bg(true);

        // External tick while thread is running (thread is sleeping)
        let result = bg.tick();
        assert!(result.next_delay.as_secs_f64() > 0.0);

        // Clean up the thread
        bg.stop();
    }

    #[test]
    fn test_thread_disabled_close_safe() {
        let mut bg = make_empty_test_bg(false);
        // stop() on a no-thread bg should be a no-op (no panic)
        bg.stop();
        // verify tick/next_delay still work after stop
        let result = bg.tick();
        assert!(result.next_delay.as_secs_f64() > 0.0);
    }

    #[test]
    fn test_concurrent_external_ticks_serialized() {
        let bg = Arc::new(make_empty_test_bg(false));
        let mut handles = vec![];

        for _ in 0..4 {
            let bg_clone = Arc::clone(&bg);
            handles.push(std::thread::spawn(move || {
                let result = bg_clone.tick();
                result.executed_tasks
            }));
        }

        let mut total = 0usize;
        for h in handles {
            total += h.join().unwrap();
        }
        // All ticks should have executed without panic.
        // With all state initialized to now, only the first might have 0 flush.
        // This just verifies serialization doesn't deadlock.
        let _ = total;
    }

    #[test]
    fn test_next_delay_during_tick() {
        let bg = Arc::new(make_empty_test_bg(false));
        let bg_tick = Arc::clone(&bg);

        // Start a tick in another thread
        let tick_handle = std::thread::spawn(move || bg_tick.tick());

        // While tick is (likely) running, call next_delay
        // This should eventually succeed (either returns immediately or
        // blocks briefly until tick completes)
        let delay = bg.next_delay();
        // Verify next_delay completes without deadlock and returns a bounded value
        assert!(
            delay <= Duration::from_secs(600),
            "next_delay should be bounded by flush_interval (600s), got {:?}",
            delay
        );

        let _ = tick_handle.join();
    }

    #[test]
    fn test_next_delay_does_not_wait_for_dataset_lock_during_tick() {
        let datasets = Arc::new(RwLock::new(HashMap::new()));
        let (key, ds_arc) = make_test_dataset("timslite_bg_next_delay_nonblocking");
        datasets.write().unwrap().insert(key, Arc::clone(&ds_arc));

        let bg = Arc::new(BackgroundTasks::new(
            datasets,
            crate::dataset::DataSetRuntimeContext::new_flush_queue(),
            Arc::new(JournalManager::Disabled),
            Arc::new(BlockCache::new(0)),
            Duration::from_millis(1),
            Duration::from_secs(1800),
            Duration::from_secs(1800),
            0,
        ));

        {
            let mut state = bg.state.lock().unwrap();
            state.last_flush = Instant::now() - Duration::from_secs(10);
        }

        let bg_tick = Arc::clone(&bg);
        let tick_started = Arc::new(AtomicBool::new(false));
        let tick_started_thread = Arc::clone(&tick_started);
        let tick_handle = std::thread::spawn(move || {
            tick_started_thread.store(true, Ordering::SeqCst);
            bg_tick.tick()
        });

        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            let state = bg.state.lock().unwrap();
            if state.flush_running {
                break;
            }
            drop(state);
            std::thread::yield_now();
        }
        assert!(tick_started.load(Ordering::SeqCst));

        let started = Instant::now();
        let _ = bg.next_delay();
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "next_delay waited for dataset lock instead of only the state lock"
        );

        let result = tick_handle.join().unwrap();
        assert_eq!(result.executed_tasks, 1);
    }

    #[test]
    fn test_flush_drains_queue_without_locking_unqueued_dataset() {
        let datasets = Arc::new(RwLock::new(HashMap::new()));
        let flush_queue = crate::dataset::DataSetRuntimeContext::new_flush_queue();
        let (queued_key, queued_ds) = make_test_dataset("timslite_bg_flush_queued");
        let (unqueued_key, unqueued_ds) = make_test_dataset("timslite_bg_flush_unqueued");

        queued_ds
            .set_runtime_context(crate::dataset::DataSetRuntimeContext::new(
                None,
                None,
                Some(Arc::clone(&flush_queue)),
            ))
            .unwrap();
        queued_ds.write(1, b"queued").unwrap();

        {
            let mut guard = datasets.write().unwrap();
            guard.insert(queued_key, Arc::clone(&queued_ds));
            guard.insert(unqueued_key, Arc::clone(&unqueued_ds));
        }

        let bg = Arc::new(BackgroundTasks::new(
            datasets,
            flush_queue,
            Arc::new(JournalManager::Disabled),
            Arc::new(BlockCache::new(0)),
            Duration::from_millis(1),
            Duration::from_secs(1800),
            Duration::from_secs(1800),
            0,
        ));
        {
            let mut state = bg.state.lock().unwrap();
            state.last_flush = Instant::now() - Duration::from_secs(10);
            // Push retention far into the future so run_retention_reclaim
            // does not fire and attempt to lock all datasets.
            state.next_retention = Instant::now() + Duration::from_secs(86400);
        }

        let bg_tick = Arc::clone(&bg);
        let tick_handle = std::thread::spawn(move || bg_tick.tick());

        std::thread::sleep(Duration::from_millis(100));
        assert!(
            tick_handle.is_finished(),
            "flush should not lock datasets without queued dirty segments"
        );

        let result = tick_handle.join().unwrap();
        assert_eq!(result.executed_tasks, 1);
    }
}
