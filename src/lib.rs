mod db;
mod graphql;
mod table;
mod utils;

use std::sync::Arc;

use async_graphql::dynamic::{Object, Schema};

/// How the library should obtain a database connection.
pub enum PoolConfig {
    /// A `postgres://` (or `postgresql://`) connection string.
    /// The library will create and own a `deadpool_postgres::Pool` from it.
    ConnectionString(String),
    /// An already-configured pool managed by the caller.
    Pool(deadpool_postgres::Pool),
}

/// Top-level configuration passed to the schema builder.
pub struct Config {
    /// Database connection — either a DSN or an existing pool.
    pub pool: PoolConfig,
    /// PostgreSQL schemas to introspect (e.g. `vec!["public".into()]`).
    pub schemas: Vec<String>,
}

/// Introspects the database described by `config` and returns a fully
/// constructed [`async_graphql::dynamic::Schema`] ready to execute queries.
pub async fn build_schema(
    config: Config,
) -> Result<Schema, Box<dyn std::error::Error + Send + Sync>> {
    // ── Resolve pool ────────────────────────────────────────────────────────
    let pool = Arc::new(match config.pool {
        PoolConfig::ConnectionString(url) => {
            let mut cfg = deadpool_postgres::Config::new();
            cfg.url = Some(url);
            cfg.create_pool(
                Some(deadpool_postgres::Runtime::Tokio1),
                tokio_postgres::NoTls,
            )?
        }
        PoolConfig::Pool(pool) => pool,
    });

    // ── Introspect ──────────────────────────────────────────────────────────
    let tables = db::introspect::get_tables(&pool, &config.schemas).await;

    // ── Assemble schema ─────────────────────────────────────────────────────
    let mut query_root = Object::new("Query");
    let mut builder = Schema::build("Query", None, None);

    // PageInfo is shared across all connection types — register it once.
    builder = builder.register(graphql::make_page_info_type());

    for table in tables {
        let table = Arc::new(table);
        let entity = graphql::generate_entity(table.clone());
        let gq = graphql::generate_query(table, pool.clone());

        query_root = query_root.field(gq.query_field);
        builder = builder
            .register(entity)
            .register(gq.condition_type)
            .register(gq.order_by_enum)
            .register(gq.connection_type)
            .register(gq.edge_type);

        for ft in gq.condition_filter_types {
            builder = builder.register(ft);
        }
    }

    let schema = builder.register(query_root).finish()?;
    Ok(schema)
}
