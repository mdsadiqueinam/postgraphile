/// A value type used in per-transaction `SET LOCAL` settings.
#[derive(Clone)]
pub enum TransactionSettingsValue {
    String(String),
    Integer(i64),
    Boolean(bool),
}

/// Per-request transaction configuration.
///
/// Inject via `Request::new(query).data(TransactionConfig { ... })` and it will
/// be applied inside the `BEGIN` / `COMMIT` block that wraps each query.
#[derive(Clone)]
pub struct TransactionConfig {
    pub isolation_level: Option<tokio_postgres::IsolationLevel>,
    pub read_only: bool,
    pub deferrable: bool,
    pub role: Option<String>,
    pub timeout_seconds: Option<u64>,
    pub settings: Vec<(String, String)>,
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
