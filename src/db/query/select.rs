use std::fmt::Write;
use std::marker::PhantomData;

use crate::TransactionConfig;
use crate::db::error::DbError;
use crate::db::JsonListExt;
use deadpool_postgres::Pool;
use tokio_postgres::types::ToSql;

use crate::db::scalar::SqlScalar;
use crate::db::transaction::{apply_settings, build_begin_statement};

use super::{QueryBase, SupportsWhere};

// ── Order-phase markers ───────────────────────────────────────────────────────

/// WHERE clauses are still allowed.
pub struct NoOrder;
/// ORDER BY has been applied; only `.execute()`, `.limit()`, `.offset()`, and more `.order_by()` are legal.
pub struct Ordered;

// ── ORDER BY direction ────────────────────────────────────────────────────────

pub enum OrderDirection {
    Asc,
    Desc,
}

impl OrderDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderDirection::Asc => "ASC",
            OrderDirection::Desc => "DESC",
        }
    }
}

// ── Select struct ─────────────────────────────────────────────────────────────

pub struct Select<O = NoOrder> {
    table: String,
    params: Vec<Option<SqlScalar>>,
    where_clause: String,
    pool: Pool,
    limit: Option<SqlScalar>,
    offset: Option<SqlScalar>,
    orders: Vec<(String, OrderDirection)>,
    _order: PhantomData<O>,
}

// ── QueryBase ─────────────────────────────────────────────────────────────────

impl<O> QueryBase for Select<O> {
    fn table(&self) -> &str { &self.table }
    fn get_where_clause(&self) -> &str { &self.where_clause }
    fn get_where_clause_mut(&mut self) -> &mut String { &mut self.where_clause }
    fn params(&self) -> &[Option<SqlScalar>] { &self.params }
    fn params_mut(&mut self) -> &mut Vec<Option<SqlScalar>> { &mut self.params }
    fn pool(&self) -> &Pool { &self.pool }
}

// Only NoOrder gets WHERE support
impl SupportsWhere for Select<NoOrder> {}

// ── Constructor ───────────────────────────────────────────────────────────────

impl Select<NoOrder> {
    pub fn new(table: &str, pool: Pool) -> Self {
        Self {
            table: table.to_string(),
            params: Vec::new(),
            where_clause: String::new(),
            pool,
            limit: None,
            offset: None,
            orders: Vec::new(),
            _order: PhantomData,
        }
    }
}

// ── Methods available in ANY order phase ──────────────────────────────────────

impl<O> Select<O> {
    /// Transition into a different order-phase without copying data.
    fn into_phase<O2>(self) -> Select<O2> {
        Select {
            table: self.table,
            params: self.params,
            where_clause: self.where_clause,
            pool: self.pool,
            limit: self.limit,
            offset: self.offset,
            orders: self.orders,
            _order: PhantomData,
        }
    }

    fn where_params(&self) -> Vec<&(dyn ToSql + Sync)> {
        self.params
            .iter()
            .map(|p| p as &(dyn ToSql + Sync))
            .collect()
    }

    fn select_params(&self) -> Vec<&(dyn ToSql + Sync)> {
        let mut params = self.where_params();
        if let Some(limit) = &self.limit {
            params.push(limit as &(dyn ToSql + Sync));
        }
        if let Some(offset) = &self.offset {
            params.push(offset as &(dyn ToSql + Sync));
        }
        params
    }

    pub fn limit(mut self, limit: i32) -> Self {
        self.limit = Some(SqlScalar::Int4(limit));
        self
    }

    pub fn offset(mut self, offset: i32) -> Self {
        self.offset = Some(SqlScalar::Int4(offset));
        self
    }

    /// Append an ORDER BY column. Returns `Select<Ordered>` so that
    /// WHERE clauses are locked out, but further `order_by`/`limit`/`offset` calls still work.
    pub fn order_by(mut self, column: &str, direction: OrderDirection) -> Select<Ordered> {
        self.orders.push((column.to_string(), direction));
        self.into_phase()
    }

    pub fn get_count_query(&self) -> String {
        if self.where_clause.is_empty() {
            format!("SELECT COUNT(*) FROM {}", self.table)
        } else {
            format!("SELECT COUNT(*) FROM {}{}", self.table, self.where_clause)
        }
    }

    pub fn get_order_clause(&self) -> String {
        if self.orders.is_empty() {
            String::new()
        } else {
            let parts: Vec<String> = self
                .orders
                .iter()
                .map(|(col, dir)| format!("{col} {}", dir.as_str()))
                .collect();
            format!(" ORDER BY {}", parts.join(", "))
        }
    }

    pub fn get_select_query(&self) -> String {
        let mut q = if self.where_clause.is_empty() {
            format!("SELECT * FROM {}", self.table)
        } else {
            format!("SELECT * FROM {}{}", self.table, self.where_clause)
        };

        let order = self.get_order_clause();
        if !order.is_empty() {
            write!(q, "{order}").unwrap();
        }

        let mut next_param = self.params.len() + 1;
        if self.limit.is_some() {
            write!(q, " LIMIT ${next_param}").unwrap();
            next_param += 1;
        }
        if self.offset.is_some() {
            write!(q, " OFFSET ${next_param}").unwrap();
        }
        q
    }

    pub async fn execute(
        &self,
        tx_config: Option<TransactionConfig>,
    ) -> Result<(i64, Vec<serde_json::Value>), DbError> {
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

        let count_q = self.get_count_query();
        let data_q = self.get_select_query();
        let where_p = self.where_params();
        let select_p = self.select_params();

        let result = tokio::try_join!(
            client.query_one(&count_q, &where_p),
            client.query(&data_q, &select_p),
        )
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

        result.map(|(count_row, data_rows)| {
            let total_count: i64 = count_row.get(0);
            let rows = data_rows.to_json_list();
            (total_count, rows)
        })
    }
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
    fn test_select_simple() {
        let q = Select::new("users", test_pool());
        assert_eq!(q.get_select_query(), "SELECT * FROM users");
        assert_eq!(q.get_count_query(), "SELECT COUNT(*) FROM users");
    }

    #[test]
    fn test_select_with_where() {
        let mut q = Select::new("users", test_pool());
        q.where_clause("id", Op::Eq, Some(SqlScalar::Int4(42)));
        let sql = q.get_select_query();
        assert!(sql.starts_with("SELECT * FROM users"));
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("$1"));
    }

    #[test]
    fn test_select_with_multiple_where() {
        let mut q = Select::new("orders", test_pool());
        q.where_clause("status", Op::Eq, Some(SqlScalar::Text("active".into())));
        q.where_clause("amount", Op::Gt, Some(SqlScalar::Int4(100)));
        let sql = q.get_select_query();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("AND"));
        assert!(sql.contains("$2"));
    }

    #[test]
    fn test_select_with_or_where() {
        let mut q = Select::new("users", test_pool());
        q.where_clause("status", Op::Eq, Some(SqlScalar::Text("active".into())));
        q.or_where_clause("status", Op::Eq, Some(SqlScalar::Text("pending".into())));
        let sql = q.get_select_query();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("OR"));
    }

    #[test]
    fn test_select_with_where_block() {
        let mut q = Select::new("products", test_pool());
        q.where_block(|q| {
            q.where_clause("id", Op::Gt, Some(SqlScalar::Int4(1)));
            q.or_where_clause("id", Op::Lt, Some(SqlScalar::Int4(100)));
        });
        let sql = q.get_select_query();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("("));
        assert!(sql.contains(")"));
        assert!(sql.contains("OR"));
    }

    #[test]
    fn test_select_with_limit() {
        let q = Select::new("users", test_pool()).limit(10);
        assert!(q.get_select_query().contains("LIMIT $1"));
    }

    #[test]
    fn test_select_with_offset() {
        let q = Select::new("users", test_pool()).offset(20);
        assert!(q.get_select_query().contains("OFFSET $1"));
    }

    #[test]
    fn test_select_with_limit_and_offset() {
        let q = Select::new("users", test_pool()).limit(10).offset(20);
        let sql = q.get_select_query();
        let limit_pos = sql.find("LIMIT").expect("no LIMIT");
        let offset_pos = sql.find("OFFSET").expect("no OFFSET");
        assert!(limit_pos < offset_pos);
    }

    #[test]
    fn test_select_with_where_limit_offset() {
        let mut q = Select::new("users", test_pool());
        q.where_clause("active", Op::Eq, Some(SqlScalar::Bool(true)));
        let q = q.limit(10).offset(5);
        let sql = q.get_select_query();
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("LIMIT $2"));
        assert!(sql.contains("OFFSET $3"));
    }

    #[test]
    fn test_order_direction() {
        assert_eq!(OrderDirection::Asc.as_str(), "ASC");
        assert_eq!(OrderDirection::Desc.as_str(), "DESC");
    }

    #[test]
    fn test_select_no_order() {
        let q = Select::new("users", test_pool());
        assert_eq!(q.get_order_clause(), "");
    }

    #[test]
    fn test_select_single_order() {
        let q = Select::new("users", test_pool())
            .order_by("name", OrderDirection::Asc);
        let clause = q.get_order_clause();
        assert!(clause.contains("ORDER BY"));
        assert!(clause.contains("name ASC"));
    }

    #[test]
    fn test_select_multiple_order() {
        let q = Select::new("users", test_pool())
            .order_by("created_at", OrderDirection::Desc)
            .order_by("id", OrderDirection::Asc);
        let clause = q.get_order_clause();
        assert!(clause.contains("created_at DESC"));
        assert!(clause.contains("id ASC"));
        assert!(clause.contains(","));
    }

    #[test]
    fn test_select_order_appears_in_query() {
        let q = Select::new("posts", test_pool())
            .order_by("date", OrderDirection::Desc);
        assert!(q.get_select_query().contains("ORDER BY date DESC"));
    }

    #[test]
    fn test_select_order_before_limit() {
        let q = Select::new("posts", test_pool())
            .order_by("id", OrderDirection::Asc)
            .limit(10);
        let sql = q.get_select_query();
        assert!(sql.find("ORDER BY").unwrap() < sql.find("LIMIT").unwrap());
    }

    #[test]
    fn test_select_full_pipeline() {
        let mut q = Select::new("orders", test_pool());
        q.where_clause("status", Op::Eq, Some(SqlScalar::Text("paid".into())));
        let q = q
            .order_by("created_at", OrderDirection::Desc)
            .order_by("id", OrderDirection::Asc)
            .limit(25)
            .offset(50);

        let sql = q.get_select_query();
        let select_pos = sql.find("SELECT").unwrap();
        let where_pos = sql.find("WHERE").unwrap();
        let order_pos = sql.find("ORDER BY").unwrap();
        let limit_pos = sql.find("LIMIT").unwrap();
        let offset_pos = sql.find("OFFSET").unwrap();

        assert!(select_pos < where_pos);
        assert!(where_pos < order_pos);
        assert!(order_pos < limit_pos);
        assert!(limit_pos < offset_pos);
    }

    #[test]
    fn test_count_query_with_where() {
        let mut q = Select::new("users", test_pool());
        q.where_clause("active", Op::Eq, Some(SqlScalar::Bool(true)));
        let sql = q.get_count_query();
        assert!(sql.starts_with("SELECT COUNT(*) FROM users"));
        assert!(sql.contains("WHERE"));
    }

    #[test]
    fn test_schema_qualified() {
        let q = Select::new("public.users", test_pool());
        assert!(q.get_select_query().contains("public.users"));
    }

    #[test]
    fn test_where_is_null() {
        let mut q = Select::new("users", test_pool());
        q.where_clause("deleted_at", Op::Eq, None);
        assert!(q.get_select_query().contains("deleted_at IS"));
    }
}
