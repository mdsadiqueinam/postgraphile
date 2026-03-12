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

pub enum TransactionSettingsValue {
    String(String),
    Integer(i64),
    Boolean(bool),
}

pub struct TransactionConfig {
    pub isolation_level: Option<tokio_postgres::IsolationLevel>,
    pub read_only: bool,
    pub deferrable: bool,
    pub role: Option<String>,
    pub timeout_seconds: Option<u64>,
    pub settings: Vec<(String, TransactionSettingsValue)>,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            isolation_level: None,
            read_only: false,
            deferrable: false,
            role: None,
            timeout_seconds: None,
            settings: Vec::new(),
        }
    }
}
