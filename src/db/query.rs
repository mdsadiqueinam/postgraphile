use crate::TransactionConfig;
use deadpool_postgres::Pool;

use super::scalar::SqlScalar;
use super::where_clause::WhereInternal;

pub struct MutationMode;

pub struct SelectMode;

pub struct Query<M> {
    query: String,
    params: Vec<Option<SqlScalar>>,
    has_where: bool,
    pool: Pool,
    _mode: std::marker::PhantomData<M>, // Tells Rust M is used here
}

// impl std::future::Future<Output = Result<(), async_graphql::Error>> + 'a

// Common implementation for BOTH
impl<M> Query<M> {
    fn new(base_sql: String, pool: Pool) -> Self {
        Self {
            query: base_sql,
            params: Vec::new(),
            pool,
            has_where: false,
            _mode: std::marker::PhantomData,
        }
    }

    async fn execute(&self, tx_config: &TransactionConfig) -> Result<(), async_graphql::Error> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| format!("Pool error: {e}"))?;

        Ok(())
    }
}

impl<M> WhereInternal for Query<M> {
    fn get_has_where(&self) -> bool {
        self.has_where
    }
    fn set_has_where(&mut self, val: bool) {
        self.has_where = val;
    }
    fn get_query(&self) -> &str {
        &self.query
    }
    fn push_to_query(&mut self, q: String) {
        self.query.push_str(&q);
    }
    fn push_param(&mut self, scalar: Option<SqlScalar>) -> usize {
        self.params.push(scalar);
        self.params.len()
    }
}

// Only Select queries get this!
impl Query<SelectMode> {
    pub fn order_by(mut self, column: &str) -> Self {
        self.query.push_str(&format!(" ORDER BY {column}"));
        self
    }
}
