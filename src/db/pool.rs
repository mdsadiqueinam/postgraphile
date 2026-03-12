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
