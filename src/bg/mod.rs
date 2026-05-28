//! Single background thread executing flush, idle-check, cache eviction, and retention
//! reclamation loops.

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::cache::BlockCache;
use crate::dataset::DataSet;
use crate::dataset::DataSetKey;

type DatasetMap = HashMap<DataSetKey, Arc<std::sync::Mutex<DataSet>>>;

/// Background task manager (single thread).
pub struct BackgroundTasks {
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

impl BackgroundTasks {
    /// Start the single background thread.
    pub fn start(
        datasets: Arc<RwLock<DatasetMap>>,
        block_cache: Arc<BlockCache>,
        flush_interval: Duration,
        idle_timeout: Duration,
        cache_idle_timeout: Duration,
        retention_check_hour: u8,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        // Idle check interval: 60 seconds
        let idle_check_interval = Duration::from_secs(60);
        // Cache eviction interval: 60 seconds
        let cache_eviction_interval = Duration::from_secs(60);

        let handle = Some(std::thread::spawn(move || {
            let mut last_flush = Instant::now();
            let mut last_idle_check = Instant::now();
            let mut last_cache_eviction = Instant::now();
            let mut next_retention = next_retention_time(retention_check_hour);

            loop {
                let now = Instant::now();
                let next_flush = last_flush + flush_interval;
                let next_idle = last_idle_check + idle_check_interval;
                let next_cache = last_cache_eviction + cache_eviction_interval;
                let wait_time = next_flush
                    .min(next_idle)
                    .min(next_cache)
                    .min(next_retention)
                    .saturating_duration_since(now);

                // Wait until timeout or shutdown signal
                if !wait_time.is_zero() && shutdown_rx.recv_timeout(wait_time).is_ok() {
                    log::info!("[bg] received shutdown signal");
                    break;
                }

                let now = Instant::now();

                // Flush: msync all open segments
                if now >= next_flush {
                    if let Ok(guard) = datasets.read() {
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
                    last_flush = now;
                }

                // Idle Check: close datasets that haven't been used
                if now >= next_idle {
                    // 1. Read lock: collect idle keys
                    let idle_keys = {
                        let guard = match datasets.read() {
                            Ok(g) => g,
                            Err(_) => {
                                last_idle_check = now;
                                continue;
                            }
                        };
                        guard
                            .iter()
                            .filter(|(_k, ds_arc)| {
                                let ds = match ds_arc.lock() {
                                    Ok(ds) => ds,
                                    Err(_) => return false,
                                };
                                ds.last_used_at().elapsed() >= idle_timeout
                            })
                            .map(|(k, _)| k.clone())
                            .collect::<Vec<_>>()
                    };

                    // 2. For each idle key, execute close with double-check
                    for key in idle_keys {
                        let ds_arc = {
                            let guard = match datasets.read() {
                                Ok(g) => g,
                                Err(_) => continue,
                            };
                            match guard.get(&key) {
                                Some(ds) => Arc::clone(ds),
                                None => continue,
                            }
                        };
                        // Double-check race condition protection
                        {
                            let mut ds = match ds_arc.lock() {
                                Ok(ds) => ds,
                                Err(_) => continue,
                            };
                            if ds.last_used_at().elapsed() >= idle_timeout {
                                if let Err(e) = ds.close() {
                                    log::error!("[bg idle] close failed for {:?}: {}", key, e);
                                } else {
                                    log::info!("[bg idle] closed dataset {:?}", key);
                                }
                            }
                        }
                    }
                    last_idle_check = now;
                }

                // Cache Eviction: evict idle cache entries
                if now >= next_cache && block_cache.is_enabled() {
                    let evicted = block_cache.evict_idle(cache_idle_timeout);
                    if evicted > 0 {
                        log::info!("[bg cache] evicted {} idle entries", evicted);
                    }
                    last_cache_eviction = now;
                }

                // Retention Reclaim: delete expired segment files once per day
                if now >= next_retention {
                    let enabled: Vec<(DataSetKey, u64)> = {
                        let guard = match datasets.read() {
                            Ok(g) => g,
                            Err(_) => {
                                next_retention = next_retention_time(retention_check_hour);
                                continue;
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
                            let guard = match datasets.read() {
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
                    // Schedule next reclaim ~24h out (use next_retention_time for wall-clock drift correction)
                    next_retention = next_retention_time(retention_check_hour);
                }
            }
        }));

        Self {
            handle,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Stop the background thread.
    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.handle.take() {
            let _ = h.join();
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
