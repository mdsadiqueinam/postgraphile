use std::collections::HashMap;
use std::fmt::Write;

use crate::TransactionConfig;
use crate::db::error::DbError;
use deadpool_postgres::Pool;
use tokio_postgres::types::ToSql;

use crate::db::scalar::SqlScalar;
use crate::db::transaction::{apply_settings, build_begin_statement};

use super::{QueryBase, SupportsWhere};

// ── Update struct ─────────────────────────────────────────────────────────────

pub struct Update {
    table: String,
    params: Vec<Option<SqlScalar>>,
    where_clause: String,
    pool: Pool,
    values: HashMap<String, Option<SqlScalar>>,
}

// ── QueryBase + SupportsWhere ─────────────────────────────────────────────────

impl QueryBase for Update {
    fn table(&self) -> &str { &self.table }
    fn get_where_clause(&self) -> &str { &self.where_clause }
    fn get_where_clause_mut(&mut self) -> &mut String { &mut self.where_clause }
    fn params(&self) -> &[Option<SqlScalar>] { &self.params }
    fn params_mut(&mut self) -> &mut Vec<Option<SqlScalar>> { &mut self.params }
    fn pool(&self) -> &Pool { &self.pool }
}

impl SupportsWhere for Update {}

// ── Constructor & methods ─────────────────────────────────────────────────────

impl Update {
    pub fn new(table: &str, pool: Pool) -> Self {
        Self {
            table: table.to_string(),
            params: Vec::new(),
            where_clause: String::new(),
            pool,
            values: HashMap::new(),
        }
    }

    /// Set a column to a value. These become the `SET col=$N` assignments.
    pub fn set(&mut self, column: &str, value: Option<SqlScalar>) -> &mut Self {
        self.values.insert(column.to_string(), value);
        self
    }

    fn all_params(&self) -> Vec<&(dyn ToSql + Sync)> {
        // SET params come first, then WHERE params
        let mut params: Vec<&(dyn ToSql + Sync)> = self
            .values
            .values()
            .map(|v| v as &(dyn ToSql + Sync))
            .collect();
        for p in &self.params {
            params.push(p as &(dyn ToSql + Sync));
        }
        params
    }

    pub fn get_query(&self) -> String {
        let mut q = format!("UPDATE {}", self.table);

        // Build SET clause: SET col1=$1, col2=$2, ...
        if !self.values.is_empty() {
            q.push_str(" SET ");
            let mut idx = 1;
            let mut first = true;
            for col in self.values.keys() {
                if !first {
                    q.push_str(", ");
                }
                write!(q, "{col} = ${idx}").unwrap();
                idx += 1;
                first = false;
            }
        }

        // Append WHERE clause (params shifted by number of SET values)
        if !self.where_clause.is_empty() {
            // Shift WHERE param indices by the number of SET values
            let shift = self.values.len();
            let shifted_where = shift_param_indices(&self.where_clause, shift);
            q.push_str(&shifted_where);
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

// ── Helper: shift $N references in a WHERE clause ─────────────────────────────

fn shift_param_indices(where_clause: &str, shift: usize) -> String {
    if shift == 0 {
        return where_clause.to_string();
    }

    let mut result = String::with_capacity(where_clause.len());
    let bytes = where_clause.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            // Parse the number after $
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            let num: usize = where_clause[start..end].parse().unwrap();
            write!(result, "${}", num + shift).unwrap();
            i = end;
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }

    result
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::operator::Op;
    use crate::db::where_clause::WhereBuilder;

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
    fn test_update_simple() {
        let q = Update::new("users", test_pool());
        assert_eq!(q.get_query(), "UPDATE users");
    }

    #[test]
    fn test_update_with_set() {
        let mut q = Update::new("users", test_pool());
        q.set("name", Some(SqlScalar::Text("Alice".into())));
        let sql = q.get_query();
        assert!(sql.starts_with("UPDATE users SET"));
        assert!(sql.contains("name = $1"));
    }

    #[test]
    fn test_update_with_set_and_where() {
        let mut q = Update::new("users", test_pool());
        q.set("name", Some(SqlScalar::Text("Alice".into())));
        q.where_clause("id", Op::Eq, Some(SqlScalar::Int4(5)));
        let sql = q.get_query();
        assert!(sql.contains("UPDATE users SET"));
        assert!(sql.contains("name = $1"));
        assert!(sql.contains("WHERE"));
        // WHERE param should be shifted: $1 in where_clause becomes $2
        assert!(sql.contains("$2"));
        assert!(!sql.contains("UPDATE FROM"));
    }

    #[test]
    fn test_update_with_multiple_set() {
        let mut q = Update::new("users", test_pool());
        q.set("name", Some(SqlScalar::Text("Bob".into())));
        q.set("age", Some(SqlScalar::Int4(30)));
        let sql = q.get_query();
        assert!(sql.contains("SET"));
        // Both columns should have unique param indices
        assert!(sql.contains("$1"));
        assert!(sql.contains("$2"));
    }

    #[test]
    fn test_shift_param_indices() {
        assert_eq!(shift_param_indices(" WHERE id = $1", 2), " WHERE id = $3");
        assert_eq!(
            shift_param_indices(" WHERE id = $1 AND name = $2", 3),
            " WHERE id = $4 AND name = $5"
        );
        assert_eq!(shift_param_indices(" WHERE x = $10", 5), " WHERE x = $15");
        assert_eq!(shift_param_indices("", 5), "");
        assert_eq!(shift_param_indices(" WHERE id = $1", 0), " WHERE id = $1");
    }
}
