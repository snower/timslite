//! Background task management (flush + idle loops).

pub mod flush;
pub mod idle;

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::dataset::DataSet;
use crate::dataset::DataSetKey;
use crate::error::Result;

type DatasetMap = HashMap<DataSetKey, Arc<std::sync::Mutex<DataSet>>>;

/// Background task manager.
pub struct BackgroundTasks {
    pub flush_handle: Option<JoinHandle<()>>,
    pub idle_handle: Option<JoinHandle<()>>,
    pub shutdown_tx_flush: Option<mpsc::Sender<()>>,
    pub shutdown_tx_idle: Option<mpsc::Sender<()>>,
}

impl BackgroundTasks {
    /// Start background tasks.
    pub fn start(
        datasets: Arc<RwLock<DatasetMap>>,
        flush_interval: Duration,
        idle_timeout: Duration,
    ) -> Self {
        let (shutdown_tx_flush, shutdown_rx_flush) = mpsc::channel();
        let (shutdown_tx_idle, shutdown_rx_idle) = mpsc::channel();

        let flush_handle = Some(flush::spawn_flush_loop(
            Arc::clone(&datasets),
            flush_interval,
            shutdown_rx_flush,
        ));

        let idle_handle = Some(idle::spawn_idle_loop(
            Arc::clone(&datasets),
            idle_timeout,
            shutdown_rx_idle,
        ));

        Self {
            flush_handle,
            idle_handle,
            shutdown_tx_flush: Some(shutdown_tx_flush),
            shutdown_tx_idle: Some(shutdown_tx_idle),
        }
    }

    /// Stop all background tasks.
    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx_flush.take() {
            let _ = tx.send(());
        }
        if let Some(tx) = self.shutdown_tx_idle.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.flush_handle.take() {
            let _ = h.join();
        }
        if let Some(h) = self.idle_handle.take() {
            let _ = h.join();
        }
    }
}
