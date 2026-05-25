//! Single background thread executing both flush and idle-check loops.

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::dataset::DataSet;
use crate::dataset::DataSetKey;

type DatasetMap = HashMap<DataSetKey, Arc<std::sync::Mutex<DataSet>>>;

/// Background task manager (single thread).
pub struct BackgroundTasks {
    handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl BackgroundTasks {
    /// Start the single background thread.
    pub fn start(
        datasets: Arc<RwLock<DatasetMap>>,
        flush_interval: Duration,
        idle_timeout: Duration,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        // Idle check interval: 60 seconds
        let idle_check_interval = Duration::from_secs(60);

        let handle = Some(std::thread::spawn(move || {
            let mut last_flush = Instant::now();
            let mut last_idle_check = Instant::now();

            loop {
                let now = Instant::now();
                let next_flush = last_flush + flush_interval;
                let next_idle = last_idle_check + idle_check_interval;
                let wait_time = next_flush.min(next_idle).saturating_duration_since(now);

                // Wait until timeout or shutdown signal
                if !wait_time.is_zero() {
                    if shutdown_rx.recv_timeout(wait_time).is_ok() {
                        log::info!("[bg] received shutdown signal");
                        break;
                    }
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
