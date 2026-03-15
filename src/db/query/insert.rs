use std::collections::HashMap;
use std::fmt::Write;

use crate::TransactionConfig;
use crate::db::error::DbError;
use deadpool_postgres::Pool;
use tokio_postgres::types::ToSql;

use crate::db::scalar::SqlScalar;
use crate::db::transaction::{apply_settings, build_begin_statement};

use super::QueryBase;

// ── Insert struct ─────────────────────────────────────────────────────────────
// Insert does NOT implement SupportsWhere — no WHERE clause on inserts.

pub struct Insert {
    table: String,
    params: Vec<Option<SqlScalar>>,
    pool: Pool,
    values: Vec<HashMap<String, Option<SqlScalar>>>,
}

// ── QueryBase (no SupportsWhere) ──────────────────────────────────────────────

impl QueryBase for Insert {
    fn table(&self) -> &str { &self.table }
    fn get_where_clause(&self) -> &str { "" }
    fn get_where_clause_mut(&mut self) -> &mut String {
        // Insert never uses a where clause; this should never be called.
        // Since SupportsWhere is NOT implemented, WhereBuilder methods
        // are never available and this is never called.
        unreachable!("Insert does not support WHERE clauses")
    }
    fn params(&self) -> &[Option<SqlScalar>] { &self.params }
    fn params_mut(&mut self) -> &mut Vec<Option<SqlScalar>> { &mut self.params }
    fn pool(&self) -> &Pool { &self.pool }
}

// ── Constructor & methods ─────────────────────────────────────────────────────

impl Insert {
    pub fn new(table: &str, pool: Pool) -> Self {
        Self {
            table: table.to_string(),
            params: Vec::new(),
            pool,
            values: Vec::new(),
        }
    }

    /// Add a row of values to insert.
    pub fn values(&mut self, row: HashMap<String, Option<SqlScalar>>) -> &mut Self {
        self.values.push(row);
        self
    }

    fn all_params(&self) -> Vec<&(dyn ToSql + Sync)> {
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        for row in &self.values {
            for val in row.values() {
                params.push(val as &(dyn ToSql + Sync));
            }
        }
        params
    }

    /// Get the ordered column list from the first row.
    fn columns(&self) -> Vec<&String> {
        if let Some(first_row) = self.values.first() {
            first_row.keys().collect()
        } else {
            Vec::new()
        }
    }

    pub fn get_query(&self) -> String {
        if self.values.is_empty() {
            return format!("INSERT INTO {} DEFAULT VALUES", self.table);
        }

        let columns = self.columns();
        let col_list = columns
            .iter()
            .map(|c| c.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        let mut q = format!("INSERT INTO {} ({col_list}) VALUES ", self.table);
        let num_cols = columns.len();
        let mut param_idx = 1;

        for (row_idx, _row) in self.values.iter().enumerate() {
            if row_idx > 0 {
                q.push_str(", ");
            }
            q.push('(');
            for col_idx in 0..num_cols {
                if col_idx > 0 {
                    q.push_str(", ");
                }
                write!(q, "${param_idx}").unwrap();
                param_idx += 1;
            }
            q.push(')');
        }

        q
    }

    pub async fn execute(
        &self,
        tx_config: Option<TransactionConfig>,
    ) -> Result<u64, DbError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| DbError::Pool(e.to_string()))?;

        let begin = build_begin_statement(&tx_config);
        client
            .batch_execute(&begin)
            .await
            .map_err(|e| DbError::Transaction(format!("BEGIN error: {e}")))?;

        if let Some(ref cfg) = tx_config {
            apply_settings(&*client, cfg).await.map_err(|e| DbError::Transaction(e.to_string()))?;
        }

        let query = self.get_query();
        let params = self.all_params();

        let result = client
            .execute(&query, &params)
            .await
            .map_err(|e| DbError::Query(e.to_string()));

        match &result {
            Ok(_) => {
                client
                    .batch_execute("COMMIT")
                    .await
                    .map_err(|e| DbError::Transaction(format!("COMMIT error: {e}")))?;
            }
            Err(_) => {
                let _ = client.batch_execute("ROLLBACK").await;
            }
        }

        result
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pool() -> Pool {
        let cfg = deadpool_postgres::Config {
            url: Some("postgres://test:test@localhost/test".to_string()),
            ..Default::default()
        };
        cfg.create_pool(
            Some(deadpool_postgres::Runtime::Tokio1),
            tokio_postgres::NoTls,
        )
        .expect("failed to create test pool")
    }

    #[test]
    fn test_insert_default_values() {
        let q = Insert::new("users", test_pool());
        assert_eq!(q.get_query(), "INSERT INTO users DEFAULT VALUES");
    }

    #[test]
    fn test_insert_single_row() {
        let mut q = Insert::new("users", test_pool());
        let mut row = HashMap::new();
        row.insert("name".to_string(), Some(SqlScalar::Text("Alice".into())));
        row.insert("age".to_string(), Some(SqlScalar::Int4(30)));
        q.values(row);

        let sql = q.get_query();
        assert!(sql.starts_with("INSERT INTO users ("));
        assert!(sql.contains("VALUES"));
        assert!(sql.contains("$1"));
        assert!(sql.contains("$2"));
    }

    #[test]
    fn test_insert_multiple_rows() {
        let mut q = Insert::new("users", test_pool());

        let mut row1 = HashMap::new();
        row1.insert("name".to_string(), Some(SqlScalar::Text("Alice".into())));
        q.values(row1);

        let mut row2 = HashMap::new();
        row2.insert("name".to_string(), Some(SqlScalar::Text("Bob".into())));
        q.values(row2);

        let sql = q.get_query();
        assert!(sql.contains("VALUES"));
        assert!(sql.contains("($1)"));
        assert!(sql.contains("($2)"));
    }

    #[test]
    fn test_insert_schema_qualified() {
        let q = Insert::new("public.users", test_pool());
        assert!(q.get_query().contains("public.users"));
    }
}
