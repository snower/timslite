pub mod config;
pub mod dataset;
pub mod errors;
pub mod queue;
pub mod query;
pub mod store;
pub mod types;

use napi_derive::napi;

#[napi]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
