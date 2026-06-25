uniffi::include_scaffolding!("timslite");

mod bridge;
mod config;
mod errors;
mod query;
mod queue;

// Re-export all UDL-defined types so the scaffolding can find them at the crate root.
pub use bridge::{
    DataSetInfo, DataSetInspectResult, DataSetState, DatasetBridge, JournalQueueBridge,
    JournalQueueConsumerBridge, JournalRecord, LengthEntry, QueueBridge, Record, StoreBridge,
    TickResult,
};
pub use config::{
    CreateDatasetOptions, DatasetConfig, QueueConsumerConfig, QueueConsumerOptions, StoreConfig,
};
pub use errors::TmslError;
pub use query::{QueryIteratorBridge, QueryLengthIteratorBridge};
pub use queue::QueueConsumerBridge;

fn version() -> String {
    timslite::VERSION.to_string()
}
