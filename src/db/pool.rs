use deadpool_postgres::Pool;

use super::query::{MutationMode, NoOrder, Query, SelectMode};
use crate::models::config::PoolConfig;

/// Resolves a [`PoolConfig`] into a concrete `deadpool_postgres::Pool`.
pub(crate) fn resolve(
    config: PoolConfig,
) -> Result<deadpool_postgres::Pool, Box<dyn std::error::Error + Send + Sync>> {
    match config {
        PoolConfig::ConnectionString(url) => {
            let mut cfg = deadpool_postgres::Config::new();
            cfg.url = Some(url);
            Ok(cfg.create_pool(
                Some(deadpool_postgres::Runtime::Tokio1),
                tokio_postgres::NoTls,
            )?)
        }
        PoolConfig::Pool(pool) => Ok(pool),
    }
}

pub trait PoolExt {
    fn select(&self, table: &str) -> Query<SelectMode, NoOrder>;
    fn insert(&self, table: &str) -> Query<MutationMode, NoOrder>;
    fn update(&self, table: &str) -> Query<MutationMode, NoOrder>;
    fn delete(&self, table: &str) -> Query<MutationMode, NoOrder>;
}

impl PoolExt for Pool {
    fn select(&self, table: &str) -> Query<SelectMode, NoOrder> {
        Query::select(table, self.clone())
    }
    fn insert(&self, table: &str) -> Query<MutationMode, NoOrder> {
        Query::insert(table, self.clone())
    }
    fn update(&self, table: &str) -> Query<MutationMode, NoOrder> {
        Query::update(table, self.clone())
    }
    fn delete(&self, table: &str) -> Query<MutationMode, NoOrder> {
        Query::delete(table, self.clone())
    }
}
