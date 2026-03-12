use async_graphql::dynamic::FieldValue;
use deadpool_postgres::Pool;
use tokio_postgres::types::ToSql;

use crate::db::JsonListExt;
use crate::db::transaction::with_transaction;
use crate::error::gql_err;
use crate::models::transaction::TransactionConfig;

use super::super::connection::{ConnectionPayload, EdgePayload, encode_cursor};
use super::super::sql_scalar::SqlScalar;

pub(super) async fn execute_connection_query(
    pool: &Pool,
    tbl_schema: &str,
    tbl_name: &str,
    where_clause: &str,
    order_clause: &str,
    params: Vec<SqlScalar>,
    limit: i64,
    offset: i64,
    order_by: &[String],
    tx_config: Option<TransactionConfig>,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
    let limit_param = params.len() + 1;
    let offset_param = params.len() + 2;

    let count_sql =
        format!("SELECT COUNT(*) FROM \"{tbl_schema}\".\"{tbl_name}\"{where_clause}");
    let data_sql = format!(
        "SELECT * FROM \"{tbl_schema}\".\"{tbl_name}\"{where_clause}{order_clause} LIMIT ${limit_param} OFFSET ${offset_param}"
    );
    let order_by = order_by.to_vec();

    with_transaction(pool, tx_config, |client| {
        Box::pin(async move {
            let base_refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
            let data_refs: Vec<&(dyn ToSql + Sync)> = base_refs
                .iter()
                .copied()
                .chain([&limit as &(dyn ToSql + Sync), &offset as _])
                .collect();

            let (count_row, data_rows) = tokio::try_join!(
                client.query_one(&count_sql, &base_refs),
                client.query(&data_sql, &data_refs),
            )
            .map_err(|e| gql_err(format!("DB query error: {e}")))?;

            let total_count: i64 = count_row.get(0);
            let json_rows = data_rows.to_json_list();
            let edge_count = json_rows.len() as i64;

            let edges = json_rows
                .into_iter()
                .enumerate()
                .map(|(i, node)| EdgePayload {
                    cursor: encode_cursor(&order_by, (offset as usize) + i),
                    node,
                })
                .collect();

            Ok(Some(FieldValue::owned_any(ConnectionPayload {
                total_count,
                has_next_page: (offset + edge_count) < total_count,
                has_previous_page: offset > 0,
                edges,
            })))
        })
    })
    .await
}
