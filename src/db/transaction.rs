use std::fmt::Write;
use std::future::Future;
use std::pin::Pin;

use deadpool_postgres::Pool;

use crate::error::gql_err;
use crate::models::transaction::TransactionConfig;

/// Acquires a pooled connection, wraps the callback in `BEGIN` / `COMMIT`, and
/// rolls back automatically on error. Works with or without a
/// [`TransactionConfig`].
pub(crate) async fn with_transaction<T>(
    pool: &Pool,
    tx_config: Option<TransactionConfig>,
    callback: impl for<'c> FnOnce(
        &'c tokio_postgres::Client,
    ) -> Pin<Box<dyn Future<Output = Result<T, async_graphql::Error>> + Send + 'c>>,
) -> Result<T, async_graphql::Error> {
    let client = pool
        .get()
        .await
        .map_err(|e| gql_err(format!("Pool error: {e}")))?;

    let begin = build_begin_statement(&tx_config);
    client
        .batch_execute(&begin)
        .await
        .map_err(|e| gql_err(format!("BEGIN error: {e}")))?;

    if let Some(ref cfg) = tx_config {
        apply_settings(&*client, cfg).await?;
    }

    let result = callback(&*client).await;

    match &result {
        Ok(_) => {
            client
                .batch_execute("COMMIT")
                .await
                .map_err(|e| gql_err(format!("COMMIT error: {e}")))?;
        }
        Err(_) => {
            let _ = client.batch_execute("ROLLBACK").await;
        }
    }

    result
}

fn build_begin_statement(tx_config: &Option<TransactionConfig>) -> String {
    let mut begin = String::from("BEGIN");
    if let Some(cfg) = tx_config {
        if let Some(level) = cfg.isolation_level {
            let lvl_str = match level {
                tokio_postgres::IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
                tokio_postgres::IsolationLevel::ReadCommitted => "READ COMMITTED",
                tokio_postgres::IsolationLevel::RepeatableRead => "REPEATABLE READ",
                tokio_postgres::IsolationLevel::Serializable => "SERIALIZABLE",
                _ => "READ COMMITTED",
            };
            write!(begin, " ISOLATION LEVEL {lvl_str}").unwrap();
        }
        if cfg.read_only {
            begin.push_str(" READ ONLY");
        }
        if cfg.deferrable {
            begin.push_str(" DEFERRABLE");
        }
    }
    begin
}

/// Applies `SET LOCAL` directives (role, custom settings, timeout) inside
/// an already-open transaction.
async fn apply_settings(
    client: &tokio_postgres::Client,
    cfg: &TransactionConfig,
) -> Result<(), async_graphql::Error> {
    if let Some(ref role) = cfg.role {
        client
            .query("SELECT set_config('role', $1, true)", &[role])
            .await
            .map_err(|e| gql_err(format!("SET ROLE error: {e}")))?;
    }

    for (key, val) in &cfg.settings {
        client
            .query("SELECT set_config($1, $2, true)", &[key, val])
            .await
            .map_err(|e| gql_err(format!("set_config error: {e}")))?;
    }

    if let Some(secs) = cfg.timeout_seconds {
        let ms = (secs * 1000).to_string();
        client
            .query("SELECT set_config('statement_timeout', $1, true)", &[&ms])
            .await
            .map_err(|e| gql_err(format!("SET timeout error: {e}")))?;
    }

    Ok(())
}
