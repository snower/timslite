//! Background flush loop: periodically msync all open segments.

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use crate::dataset::DataSet;
use crate::dataset::DataSetKey;

type DatasetMap = HashMap<DataSetKey, Arc<std::sync::Mutex<DataSet>>>;

/// Spawn the flush loop thread.
pub fn spawn_flush_loop(
    datasets: Arc<RwLock<DatasetMap>>,
    interval: Duration,
    shutdown_rx: mpsc::Receiver<()>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || loop {
        if shutdown_rx.recv_timeout(interval).is_ok() {
            log::info!("[flush] received shutdown signal");
            break;
        }

        let guard = match datasets.read() {
            Ok(g) => g,
            Err(_) => continue,
        };

        for (_key, dataset_arc) in (*guard).iter() {
            let mut ds = dataset_arc.lock().unwrap();
            if let Err(e) = ds.flush() {
                log::error!("[flush] failed: {}", e);
            }
        }
    })
}
