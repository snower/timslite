uniffi::include_scaffolding!("timslite");

mod bridge;
mod config;
mod errors;
mod query;
mod queue;

pub use bridge::{
    DatasetBridge, DataSetInfo, DataSetInspectResult, DataSetState, JournalIndexInfo,
    JournalQueueBridge, JournalQueueConsumerBridge, JournalRecord, LengthEntry, QueueBridge,
    QueueConsumerInfo, QueueConsumerInspectResult, QueueConsumerPendingEntry, QueueConsumerState,
    Record, StoreBridge, TickResult,
};
pub use config::{CreateDatasetOptions, DatasetConfig, QueueConsumerConfig, QueueConsumerOptions, StoreConfig};
pub use errors::TmslError;
pub use query::{QueryIteratorBridge, QueryLengthIteratorBridge};
pub use queue::QueueConsumerBridge;

fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
