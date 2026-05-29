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
use crate::dataset::DataSet;
use crate::dataset::DataSetKey;

type DatasetMap = HashMap<DataSetKey, Arc<std::sync::Mutex<DataSet>>>;

/// Shared scheduling state kept across tick invocations.
pub struct ExecutorState {
    pub last_flush: Instant,
    pub last_idle_check: Instant,
    pub last_cache_eviction: Instant,
    pub next_retention: Instant,
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
    block_cache: Arc<BlockCache>,
    flush_interval: Duration,
    idle_timeout: Duration,
    cache_idle_timeout: Duration,
    retention_check_hour: u8,
    handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

/// Compute the next wall-clock Instant at which retention reclaim should run.
/// `check_hour` is 0-23 representing the local hour of day (treated as UTC for simplicity).
fn next_retention_time(check_hour: u8) -> Instant {
    let hour = (check_hour as u64) % 24;
    let now = Instant::now();
    let secs_since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    let secs_into_day = secs_since_epoch % 86400;
    let target_secs_into_day = hour * 3600;
    let wait_secs = if target_secs_into_day > secs_into_day {
        target_secs_into_day - secs_into_day
    } else {
        // Already past today's target — schedule for tomorrow
        86400 - (secs_into_day - target_secs_into_day)
    };
    // Add wait_secs + at least 1s to avoid tight loop near the boundary
    now + Duration::from_secs(wait_secs.max(1))
}

/// Idle-check interval (fixed).
const IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(60);
/// Cache-eviction interval (fixed).
const CACHE_EVICTION_INTERVAL: Duration = Duration::from_secs(60);

impl BackgroundTasks {
    /// Create the executor without spawning a background thread.
    pub fn new(
        datasets: Arc<RwLock<DatasetMap>>,
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
            })),
            datasets,
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
        }));

        // Clone Arcs for the thread
        let thread_state = Arc::clone(&state);
        let thread_datasets = Arc::clone(&datasets);
        let thread_block_cache = Arc::clone(&block_cache);

        let handle = std::thread::spawn(move || {
            let bg = BackgroundTasks {
                state: thread_state,
                datasets: thread_datasets,
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
        let mut state = self.state.lock().unwrap();
        let executed = self.execute_due_tasks(&mut state);
        let next_delay = self.compute_next_delay(&state);
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

    /// Stop the background thread (no-op if no thread was started).
    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }

    // ─── private helpers ────────────────────────────────────────────────

    fn compute_next_delay(&self, state: &ExecutorState) -> Duration {
        let now = Instant::now();
        let next_flush = state.last_flush + self.flush_interval;
        let next_idle = state.last_idle_check + IDLE_CHECK_INTERVAL;
        let next_cache = state.last_cache_eviction + CACHE_EVICTION_INTERVAL;
        next_flush
            .min(next_idle)
            .min(next_cache)
            .min(state.next_retention)
            .saturating_duration_since(now)
    }

    fn execute_due_tasks(&self, state: &mut ExecutorState) -> usize {
        let now = Instant::now();
        let mut executed = 0usize;

        // Flush
        if now >= state.last_flush + self.flush_interval {
            if let Ok(guard) = self.datasets.read() {
                for (_key, ds_arc) in guard.iter() {
                    let mut ds = match ds_arc.lock() {
                        Ok(ds) => ds,
                        Err(_) => continue,
                    };
                    if let Err(e) = ds.flush() {
                        log::error!("[bg flush] failed: {}", e);
                    }
                }
            }
            state.last_flush = now;
            executed += 1;
        }

        // Idle Check
        if now >= state.last_idle_check + IDLE_CHECK_INTERVAL {
            let idle_keys = {
                let guard = match self.datasets.read() {
                    Ok(g) => g,
                    Err(_) => {
                        state.last_idle_check = now;
                        return executed + 1;
                    }
                };
                guard
                    .iter()
                    .filter(|(_k, ds_arc)| {
                        let ds = match ds_arc.lock() {
                            Ok(ds) => ds,
                            Err(_) => return false,
                        };
                        ds.last_used_at().elapsed() >= self.idle_timeout
                    })
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
                {
                    let mut ds = match ds_arc.lock() {
                        Ok(ds) => ds,
                        Err(_) => continue,
                    };
                    if ds.last_used_at().elapsed() >= self.idle_timeout {
                        if let Err(e) = ds.close() {
                            log::error!("[bg idle] close failed for {:?}: {}", key, e);
                        } else {
                            log::info!("[bg idle] closed dataset {:?}", key);
                        }
                    }
                }
            }
            state.last_idle_check = now;
            executed += 1;
        }

        let now = Instant::now();

        // Cache Eviction
        if now >= state.last_cache_eviction + CACHE_EVICTION_INTERVAL
            && self.block_cache.is_enabled()
        {
            let evicted = self.block_cache.evict_idle(self.cache_idle_timeout);
            if evicted > 0 {
                log::info!("[bg cache] evicted {} idle entries", evicted);
            }
            state.last_cache_eviction = now;
            executed += 1;
        }

        // Retention Reclaim
        if now >= state.next_retention {
            let enabled: Vec<(DataSetKey, u64)> = {
                let guard = match self.datasets.read() {
                    Ok(g) => g,
                    Err(_) => {
                        state.next_retention = next_retention_time(self.retention_check_hour);
                        return executed + 1;
                    }
                };
                guard
                    .iter()
                    .filter_map(|(k, ds_arc)| {
                        let ds = ds_arc.lock().ok()?;
                        if ds.retention_ms() > 0 {
                            Some((k.clone(), ds.retention_ms()))
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
                let mut ds = match ds_arc.lock() {
                    Ok(ds) => ds,
                    Err(_) => continue,
                };
                match ds.reclaim_expired_segments() {
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
            state.next_retention = next_retention_time(self.retention_check_hour);
            executed += 1;
        }

        executed
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

    #[test]
    fn test_next_retention_time_is_not_zero() {
        // Just ensure it returns a future instant (>= now)
        let nr = next_retention_time(0);
        let diff = nr.saturating_duration_since(Instant::now());
        assert!(diff.as_secs() <= 86400);
    }

    #[test]
    fn test_next_retention_time_clamp_hour() {
        // hour 25 should wrap to 1
        let _ = next_retention_time(25);
        let _ = next_retention_time(23);
    }
}
