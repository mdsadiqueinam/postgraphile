mod db;
mod error;
mod graphql;
mod models;
mod schema;
mod utils;

pub use db::error::DbError;
pub use models::config::{Config, PoolConfig};
pub use models::transaction::{TransactionConfig, TransactionSettingsValue};
pub use schema::TurboGraph;

/// Convenience wrapper around [`TurboGraph::new`].
pub async fn build_schema(
    config: Config,
) -> Result<TurboGraph, Box<dyn std::error::Error + Send + Sync>> {
    TurboGraph::new(config).await
}
