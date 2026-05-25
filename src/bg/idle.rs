use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use crate::dataset::DataSet;
use crate::dataset::DataSetKey;

type DatasetMap = HashMap<DataSetKey, Arc<std::sync::Mutex<DataSet>>>;

/// Spawn the idle-check loop thread.
pub fn spawn_idle_loop(
    datasets: Arc<RwLock<DatasetMap>>,
    timeout: Duration,
    shutdown_rx: mpsc::Receiver<()>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || loop {
        match shutdown_rx.recv_timeout(Duration::from_secs(60)) {
            Ok(()) => {
                log::info!("[idle] received shutdown signal");
                break;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }

        let guard = match datasets.read() {
            Ok(g) => g,
            Err(e) => continue,
        };

        let idle_keys: Vec<DataSetKey> = guard
            .iter()
            .filter(|(_k, ds_arc)| {
                let ds = ds_arc.lock().unwrap();
                ds.last_used_at().elapsed() >= timeout
            })
            .map(|(k, _ds_arc)| k.clone())
            .collect();

        drop(guard);

        for key in idle_keys {
            let dataset_arc: Arc<std::sync::Mutex<DataSet>> = {
                let guard = match datasets.read() {
                    Ok(g) => g,
                    Err(_) => continue,
                };
                match guard.get(&key) {
                    Some(ds_arc) => Arc::clone(ds_arc),
                    None => continue,
                }
            };

            {
                let mut ds = dataset_arc.lock().unwrap();
                if ds.last_used_at().elapsed() >= timeout {
                    if let Err(e) = ds.close() {
                        log::error!("[idle] close failed for {:?}: {}", key, e);
                    } else {
                        log::info!("[idle] closed dataset {:?}", key);
                    }
                }
            }
        }
    })
}
