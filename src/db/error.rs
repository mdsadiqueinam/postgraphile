use std::fmt;

#[derive(Debug, Clone)]
pub enum DbError {
    Pool(String),
    Transaction(String),
    Query(String),
    Validation(String),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Pool(msg) => write!(f, "Pool error: {}", msg),
            DbError::Transaction(msg) => write!(f, "Transaction error: {}", msg),
            DbError::Query(msg) => write!(f, "Query error: {}", msg),
            DbError::Validation(msg) => write!(f, "Validation error: {}", msg),
        }
    }
}

impl std::error::Error for DbError {}
