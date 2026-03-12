use std::sync::Arc;

use async_graphql::dynamic::Schema;
use deadpool_postgres::Pool;
use tokio::sync::RwLock;
use tokio_postgres::AsyncMessage;

/// SQL to install DDL event triggers that send NOTIFY on schema changes.
/// Requires superuser privileges.
const INSTALL_TRIGGERS_SQL: &str = r"
CREATE OR REPLACE FUNCTION turbograph_watch_ddl() RETURNS event_trigger AS $$
BEGIN
  PERFORM pg_notify('turbograph_watch', TG_TAG);
END;
$$ LANGUAGE plpgsql;

DROP EVENT TRIGGER IF EXISTS turbograph_watch_ddl;
CREATE EVENT TRIGGER turbograph_watch_ddl ON ddl_command_end
  EXECUTE FUNCTION turbograph_watch_ddl();

DROP EVENT TRIGGER IF EXISTS turbograph_watch_drop;
CREATE EVENT TRIGGER turbograph_watch_drop ON sql_drop
  EXECUTE FUNCTION turbograph_watch_ddl();
";

/// Creates the event trigger function and event triggers in PostgreSQL.
pub(crate) async fn install_triggers(
    pool: &Pool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = pool.get().await?;
    client.batch_execute(INSTALL_TRIGGERS_SQL).await?;
    Ok(())
}

/// Opens a dedicated connection for `LISTEN`, then spawns a background task
/// that rebuilds the schema whenever a DDL notification arrives.
pub(crate) async fn start_watching(
    connection_url: String,
    pool: Arc<Pool>,
    schemas: Vec<String>,
    live_schema: Arc<RwLock<Schema>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (client, mut connection) =
        tokio_postgres::connect(&connection_url, tokio_postgres::NoTls).await?;

    // Forward notifications from the connection driver to an mpsc channel.
    let (notify_tx, mut notify_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    tokio::spawn(async move {
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(AsyncMessage::Notification(n))) => {
                    if notify_tx.send(n.payload().to_string()).is_err() {
                        break;
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    eprintln!("[turbograph] watch connection error: {e}");
                    break;
                }
                None => break,
            }
        }
    });

    client.batch_execute("LISTEN turbograph_watch").await?;
    eprintln!("[turbograph] watching for schema changes");

    // Process notifications: debounce, rebuild, and swap.
    tokio::spawn(async move {
        // Keep the LISTEN client alive for the lifetime of this task.
        let _client = client;

        while let Some(tag) = notify_rx.recv().await {
            eprintln!("[turbograph] DDL change detected: {tag}");

            // Debounce: wait briefly then drain any queued notifications.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            while notify_rx.try_recv().is_ok() {}

            match crate::schema::rebuild_schema(&pool, &schemas).await {
                Ok(new_schema) => {
                    eprintln!("[turbograph] schema rebuilt successfully");
                    *live_schema.write().await = new_schema;
                }
                Err(e) => {
                    eprintln!("[turbograph] failed to rebuild schema: {e}");
                }
            }
        }
    });

    Ok(())
}
