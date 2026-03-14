use crate::TransactionConfig;
use crate::db::JsonListExt;
use deadpool_postgres::Pool;
use std::fmt::Write;
use tokio_postgres::types::ToSql;

use super::scalar::SqlScalar;
use super::transaction::{apply_settings, build_begin_statement};
use super::where_clause::WhereInternal;

// ── Mode markers ─────────────────────────────────────────────────────────────

pub struct MutationMode;
pub struct SelectMode;

// ── Order-phase markers ───────────────────────────────────────────────────────

/// The query has not yet had ORDER BY applied – WHERE clauses are still allowed.
pub struct NoOrder;
/// ORDER BY has been applied; only `.execute()` is legal now.
pub struct Ordered;

pub enum StatementType {
    Select,
    Update,
    Delete,
    Insert,
}

impl StatementType {
    fn as_str(&self) -> &'static str {
        match self {
            StatementType::Select => "SELECT",
            StatementType::Update => "UPDATE",
            StatementType::Delete => "DELETE",
            StatementType::Insert => "INSERT",
        }
    }
}

pub enum QueryResult {
    Select {
        total_count: i64,
        rows: Vec<serde_json::Value>,
    },
    Mutation,
}

// ── Query struct ──────────────────────────────────────────────────────────────

pub struct Query<M, O = NoOrder> {
    table: String,
    statement_type: StatementType,
    params: Vec<Option<SqlScalar>>,
    where_clause: String,
    has_where: bool,
    pool: Pool,
    limit: Option<SqlScalar>,
    offset: Option<SqlScalar>,
    orders: Vec<(String, OrderDirection)>,
    _mode: std::marker::PhantomData<M>,
    _order: std::marker::PhantomData<O>,
}

// ── Internal helpers (available to both modes / both order phases) ─────────────

impl<M, O> Query<M, O> {
    fn new(table: String, statement_type: StatementType, pool: Pool) -> Self {
        Self {
            table,
            statement_type,
            params: Vec::new(),
            where_clause: String::new(),
            pool,
            has_where: false,
            limit: None,
            offset: None,
            orders: Vec::new(),
            _mode: std::marker::PhantomData,
            _order: std::marker::PhantomData,
        }
    }

    /// Transition into a different order-phase without copying any data.
    fn into_phase<O2>(self) -> Query<M, O2> {
        Query {
            table: self.table,
            statement_type: self.statement_type,
            params: self.params,
            where_clause: self.where_clause,
            has_where: self.has_where,
            pool: self.pool,
            limit: self.limit,
            offset: self.offset,
            orders: self.orders,
            _mode: std::marker::PhantomData,
            _order: std::marker::PhantomData,
        }
    }

    fn count_params(&self) -> Vec<&(dyn ToSql + Sync)> {
        self.params
            .iter()
            .map(|p| p as &(dyn ToSql + Sync))
            .collect()
    }

    fn data_params(&self) -> Vec<&(dyn ToSql + Sync)> {
        let mut params = self.count_params();

        if let Some(limit) = &self.limit {
            params.push(limit as &(dyn ToSql + Sync));
        }

        if let Some(offset) = &self.offset {
            params.push(offset as &(dyn ToSql + Sync));
        }

        params
    }

    fn get_count_query(&self) -> String {
        format!(
            "{} COUNT(*) FROM {} {}",
            StatementType::Select.as_str(),
            self.table,
            self.where_clause
        )
    }

    fn get_order_clause(&self) -> String {
        if self.orders.is_empty() {
            String::new()
        } else {
            let order_strs: Vec<String> = self
                .orders
                .iter()
                .map(|(col, dir)| format!("{} {}", col, dir.as_str()))
                .collect();
            format!(" ORDER BY {}", order_strs.join(", "))
        }
    }

    fn get_data_query(&self) -> String {
        let mut query = format!(
            "{} * FROM {} {}",
            self.statement_type.as_str(),
            self.table,
            self.where_clause
        );

        let order_clause = self.get_order_clause();

        if !order_clause.is_empty() {
            write!(query, " {order_clause}").unwrap();
        }

        if self.limit.is_some() {
            write!(query, " LIMIT ${}", self.params.len() + 1).unwrap();
        }

        if self.offset.is_some() {
            write!(query, " OFFSET ${}", self.params.len() + 2).unwrap();
        }

        query
    }

    async fn execute_select_query(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<(i64, Vec<serde_json::Value>), async_graphql::Error> {
        let count_params = self.count_params();
        let data_params = self.data_params();

        let count_query = self.get_count_query();
        let data_query = self.get_data_query();

        let (count_row, data_rows) = tokio::try_join!(
            client.query_one(&count_query, &count_params),
            client.query(&data_query, &data_params),
        )
        .map_err(|e| format!("DB query error: {e}"))?;

        let total_count: i64 = count_row.get(0);
        let json_rows = data_rows.to_json_list();

        Ok((total_count, json_rows))
    }

    async fn execute_mutation_query(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<(), async_graphql::Error> {
        let data_params = self.data_params();
        let data_query = self.get_data_query();
        client
            .execute(&data_query, &data_params)
            .await
            .map_err(|e| format!("DB query error: {e}"))?;
        // Implementation for mutation query execution
        Ok(())
    }
}

// ── execute is available in all states ────────────────────────────────────────

impl<M, O> Query<M, O> {
    pub fn limit(&mut self, limit: i32) -> &mut Self {
        self.limit = Some(SqlScalar::Int4(limit));
        self
    }

    pub fn offset(&mut self, offset: i32) -> &mut Self {
        self.offset = Some(SqlScalar::Int4(offset));
        self
    }

    pub async fn execute(
        &self,
        tx_config: &Option<TransactionConfig>,
    ) -> Result<QueryResult, async_graphql::Error> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| async_graphql::Error::new(format!("Pool error: {e}")))?;

        let begin = build_begin_statement(tx_config);
        client
            .batch_execute(&begin)
            .await
            .map_err(|e| format!("BEGIN error: {e}"))?;

        if let Some(cfg) = tx_config {
            apply_settings(&*client, cfg).await?;
        }

        let result = match self.statement_type {
            StatementType::Select => {
                self.execute_select_query(&client)
                    .await
                    .map(|obj| QueryResult::Select {
                        total_count: obj.0,
                        rows: obj.1,
                    })
            }
            _ => self
                .execute_mutation_query(&client)
                .await
                .map(|_| QueryResult::Mutation),
        };

        match &result {
            Ok(_) => {
                client
                    .batch_execute("COMMIT")
                    .await
                    .map_err(|e| format!("COMMIT error: {e}"))?;
            }
            Err(_) => {
                let _ = client.batch_execute("ROLLBACK").await;
            }
        }

        result
    }
}

// ── WhereInternal (internal plumbing, both modes, NoOrder only) ───────────────

impl<M> WhereInternal for Query<M, NoOrder> {
    fn get_has_where(&self) -> bool {
        self.has_where
    }
    fn set_has_where(&mut self, val: bool) {
        self.has_where = val;
    }
    fn get_query(&self) -> &str {
        &self.where_clause
    }
    fn push_to_query(&mut self, q: String) {
        self.where_clause.push_str(&q);
    }
    fn push_param(&mut self, scalar: Option<SqlScalar>) -> usize {
        self.params.push(scalar);
        self.params.len()
    }
}

// ── order_by is available on SELECT queries in ANY order phase ───────────────
// Calling it from NoOrder advances to Ordered (locking out WHERE clauses).
// Calling it again from Ordered just appends another sort column.

impl<O> Query<SelectMode, O> {
    /// Append an ORDER BY column. Returns `Query<SelectMode, Ordered>` so that
    /// WHERE clauses are locked out after the first call, but further
    /// `order_by` calls are still allowed.
    pub fn order_by(
        mut self,
        column: &str,
        direction: OrderDirection,
    ) -> Query<SelectMode, Ordered> {
        self.orders.push((column.to_string(), direction));
        // old phase will drop here
        self.into_phase()
    }
}

// ── ORDER BY direction ────────────────────────────────────────────────────────

pub enum OrderDirection {
    Asc,
    Desc,
}

impl OrderDirection {
    fn as_str(&self) -> &'static str {
        match self {
            OrderDirection::Asc => "ASC",
            OrderDirection::Desc => "DESC",
        }
    }
}

// ── Constructors (one per mode) ───────────────────────────────────────────────

impl Query<SelectMode, NoOrder> {
    pub fn select(table: &str, pool: Pool) -> Self {
        Self::new(table.to_string(), StatementType::Select, pool)
    }
}

impl Query<MutationMode, Ordered> {
    pub fn update(table: String, pool: Pool) -> Self {
        Self::new(table, StatementType::Update, pool)
    }

    pub fn delete(table: String, pool: Pool) -> Self {
        Self::new(table, StatementType::Delete, pool)
    }

    pub fn insert(table: String, pool: Pool) -> Self {
        Self::new(table, StatementType::Insert, pool)
    }
}
