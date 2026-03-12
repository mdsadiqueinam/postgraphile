/// How the library should obtain a database connection.
pub enum PoolConfig {
    /// A `postgres://` (or `postgresql://`) connection string.
    /// The library will create and own a `deadpool_postgres::Pool` from it.
    ConnectionString(String),
    /// An already-configured pool managed by the caller.
    Pool(deadpool_postgres::Pool),
}

/// Top-level configuration passed to [`build_schema`](crate::build_schema).
pub struct Config {
    /// Database connection — either a DSN or an existing pool.
    pub pool: PoolConfig,
    /// PostgreSQL schemas to introspect (e.g. `vec!["public".into()]`).
    pub schemas: Vec<String>,
    /// When `true`, the library installs PostgreSQL event triggers and spawns
    /// a background listener that rebuilds the schema on DDL changes.
    pub watch_pg: bool,
}
