use std::collections::HashMap;
use std::sync::Arc;

use async_graphql::Value as GqlValue;
use async_graphql::dynamic::FieldValue;
use deadpool_postgres::Pool;
use tokio_postgres::types::ToSql;

use crate::db::error::DbError;
use crate::db::{JsonExt, JsonListExt};
use crate::db::transaction::with_transaction;
use crate::models::table::Column;
use crate::models::transaction::TransactionConfig;

use super::super::query::sql::build_where_clause;
use super::super::sql_scalar::SqlScalar;
use super::super::type_mapping::to_sql_scalar;

fn db_err_to_gql(err: DbError) -> async_graphql::Error {
    async_graphql::Error::new(err.to_string())
}

/// INSERT … RETURNING *  →  single entity (or null if no columns provided).
pub(super) async fn execute_create(
    pool: &Pool,
    tbl_schema: &str,
    tbl_name: &str,
    input: Vec<(String, GqlValue)>,
    columns: &[Arc<Column>],
    col_map: &HashMap<String, usize>,
    tx_config: Option<TransactionConfig>,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
    let mut col_parts = Vec::new();
    let mut placeholders = Vec::new();
    let mut params = Vec::<SqlScalar>::new();

    for (key, val) in &input {
        let Some(&idx) = col_map.get(key) else {
            continue;
        };
        let col = &columns[idx];
        if let Some(scalar) = to_sql_scalar(col, val) {
            col_parts.push(format!("\"{}\"", col.name()));
            params.push(scalar);
            placeholders.push(format!("${}", params.len()));
        }
    }

    if col_parts.is_empty() {
        return Err(async_graphql::Error::new("No valid columns provided for insert"));
    }

    let sql = format!(
        "INSERT INTO \"{}\".\"{}\" ({}) VALUES ({}) RETURNING *",
        tbl_schema,
        tbl_name,
        col_parts.join(", "),
        placeholders.join(", "),
    );

    with_transaction(pool, tx_config, |client| {
        Box::pin(async move {
            let refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

            let row = client
                .query_one(&sql, &refs)
                .await
                .map_err(|e| DbError::Query(format!("INSERT error: {e}")))?;

            Ok(Some(FieldValue::owned_any(row.to_json())))
        })
    })
    .await
    .map_err(db_err_to_gql)
}

/// UPDATE … SET … WHERE … RETURNING *  →  list of updated entities.
pub(super) async fn execute_update(
    pool: &Pool,
    tbl_schema: &str,
    tbl_name: &str,
    patch: Vec<(String, GqlValue)>,
    condition: Option<Vec<(String, GqlValue)>>,
    columns: &[Arc<Column>],
    update_col_map: &HashMap<String, usize>,
    cond_col_map: &HashMap<String, usize>,
    tx_config: Option<TransactionConfig>,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
    // Build SET clause first — params are numbered $1..$M
    let mut set_parts = Vec::new();
    let mut params = Vec::<SqlScalar>::new();

    for (key, val) in &patch {
        let Some(&idx) = update_col_map.get(key) else {
            continue;
        };
        let col = &columns[idx];
        if matches!(val, GqlValue::Null) {
            // Explicit null → SET column = NULL (no param needed)
            set_parts.push(format!("\"{}\" = NULL", col.name()));
        } else if let Some(scalar) = to_sql_scalar(col, val) {
            params.push(scalar);
            set_parts.push(format!("\"{}\" = ${}", col.name(), params.len()));
        }
    }

    if set_parts.is_empty() {
        return Err(async_graphql::Error::new("No valid columns provided for update"));
    }

    // Build WHERE clause — params continue numbering from $M+1
    let mut where_clause = String::new();
    if let Some(pairs) = condition {
        build_where_clause(&mut where_clause, &mut params, pairs, columns, cond_col_map)?;
    }

    let mut sql = format!(
        "UPDATE \"{}\".\"{}\" SET {}",
        tbl_schema,
        tbl_name,
        set_parts.join(", "),
    );
    sql.push_str(&where_clause);
    sql.push_str(" RETURNING *");

    with_transaction(pool, tx_config, |client| {
        Box::pin(async move {
            let refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

            let rows = client
                .query(&sql, &refs)
                .await
                .map_err(|e| DbError::Query(format!("UPDATE error: {e}")))?;

            let list: Vec<FieldValue> = rows
                .to_json_list()
                .into_iter()
                .map(FieldValue::owned_any)
                .collect();

            Ok(Some(FieldValue::list(list)))
        })
    })
    .await
    .map_err(db_err_to_gql)
}

/// DELETE … WHERE … RETURNING *  →  list of deleted entities.
pub(super) async fn execute_delete(
    pool: &Pool,
    tbl_schema: &str,
    tbl_name: &str,
    condition: Option<Vec<(String, GqlValue)>>,
    columns: &[Arc<Column>],
    cond_col_map: &HashMap<String, usize>,
    tx_config: Option<TransactionConfig>,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
    let mut params = Vec::<SqlScalar>::new();
    let mut where_clause = String::new();

    if let Some(pairs) = condition {
        build_where_clause(&mut where_clause, &mut params, pairs, columns, cond_col_map)?;
    }

    let mut sql = format!("DELETE FROM \"{}\".\"{}\"", tbl_schema, tbl_name);
    sql.push_str(&where_clause);
    sql.push_str(" RETURNING *");

    with_transaction(pool, tx_config, |client| {
        Box::pin(async move {
            let refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

            let rows = client
                .query(&sql, &refs)
                .await
                .map_err(|e| DbError::Query(format!("DELETE error: {e}")))?;

            let list: Vec<FieldValue> = rows
                .to_json_list()
                .into_iter()
                .map(FieldValue::owned_any)
                .collect();

            Ok(Some(FieldValue::list(list)))
        })
    })
    .await
    .map_err(db_err_to_gql)
}
