use std::collections::HashMap;
use std::sync::Arc;

use async_graphql::Value as GqlValue;
use async_graphql::dynamic::{Enum, Field, FieldFuture, InputObject, InputValue, Object, TypeRef};
use deadpool_postgres::Pool;

use crate::models::config::TransactionConfig;
use crate::models::table::Table;
use crate::utils::inflection::to_pascal_case;

use super::connection::make_connection_types;
use super::filter::{make_condition_filter_types, make_condition_type, make_order_by_enum};
use super::sql_scalar::SqlScalar;

mod executor;
mod sql;

/// Everything the schema builder needs for one table.
pub struct GeneratedQuery {
    /// The root Query field (e.g. `allUsers`).
    pub query_field: Field,
    /// The `{T}Condition` input type - must be registered with the schema.
    pub condition_type: InputObject,
    /// Per-column filter input objects referenced by `{T}Condition`.
    pub condition_filter_types: Vec<InputObject>,
    /// The `{T}OrderBy` enum - must be registered with the schema.
    pub order_by_enum: Enum,
    /// The `{T}Connection` object type - must be registered with the schema.
    pub connection_type: Object,
    /// The `{T}Edge` object type - must be registered with the schema.
    pub edge_type: Object,
}

/// Generates a root Query field (e.g. `allUsers`) with Turbograph-style
/// filtering arguments:
///
/// ```graphql
/// allUsers(
///   condition: UserCondition   # equality filter per column
///   orderBy:   [UserOrderBy]   # COLUMN_ASC / COLUMN_DESC
///   first:     Int             # LIMIT
///   offset:    Int             # OFFSET
/// ): UserConnection!
/// ```
pub fn generate_query(table: Arc<Table>, pool: Arc<Pool>) -> GeneratedQuery {
    let condition_filter_types = make_condition_filter_types(&table);
    let condition_type = make_condition_type(&table);
    let order_by_enum = make_order_by_enum(&table);
    let (connection_type, edge_type) = make_connection_types(&table);

    let connection_type_name = connection_type.type_name().to_string();
    let condition_type_name = condition_type.type_name().to_string();
    let order_by_type_name = order_by_enum.type_name().to_string();
    let field_name = format!("all{}", to_pascal_case(table.name()));
    let tbl_schema = table.schema_name().to_string();
    let tbl_name = table.name().to_string();

    let columns = Arc::new(table.columns().to_vec());
    let (mut name_map, mut upper_map) = (HashMap::new(), HashMap::new());
    for (i, col) in columns.iter().enumerate().filter(|(_, c)| !c.omit_read()) {
        name_map.insert(col.name().to_string(), i);
        upper_map.insert(col.name().to_uppercase(), i);
    }
    let col_by_name = Arc::new(name_map);
    let col_by_upper = Arc::new(upper_map);

    let query_field = Field::new(
        field_name,
        TypeRef::named_nn(connection_type_name),
        move |ctx| {
            let condition_pairs: Option<Vec<(String, GqlValue)>> = ctx
                .args
                .get("condition")
                .and_then(|v| v.object().ok())
                .map(|obj| {
                    obj.iter()
                        .map(|(k, v)| (k.to_string(), v.as_value().clone()))
                        .collect()
                });

            let order_by: Vec<String> = ctx
                .args
                .get("orderBy")
                .and_then(|v| v.list().ok())
                .map(|list| {
                    list.iter()
                        .filter_map(|item| item.enum_name().ok().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            let first = ctx.args.get("first").and_then(|v| v.i64().ok());
            let offset = ctx.args.get("offset").and_then(|v| v.i64().ok());

            let pool = pool.clone();
            let tbl_schema = tbl_schema.clone();
            let tbl_name = tbl_name.clone();
            let columns = columns.clone();
            let col_by_name = col_by_name.clone();
            let col_by_upper = col_by_upper.clone();
            let tx_config = ctx.data_opt::<TransactionConfig>().cloned();

            FieldFuture::new(async move {
                let mut where_clause = String::new();
                let mut params = Vec::<SqlScalar>::with_capacity(8);

                if let Some(pairs) = condition_pairs {
                    sql::build_where_clause(
                        &mut where_clause,
                        &mut params,
                        pairs,
                        &columns,
                        &col_by_name,
                    )?;
                }

                let mut order_clause = String::new();
                sql::build_order_by_clause(&mut order_clause, &order_by, &columns, &col_by_upper)?;

                let safe_limit = first.unwrap_or(100).clamp(1, 1000);
                let off = offset.unwrap_or(0).max(0);

                executor::execute_connection_query(
                    &pool,
                    &tbl_schema,
                    &tbl_name,
                    &where_clause,
                    &order_clause,
                    params,
                    safe_limit,
                    off,
                    &order_by,
                    tx_config,
                )
                .await
            })
        },
    )
    .argument(InputValue::new(
        "condition",
        TypeRef::named(condition_type_name),
    ))
    .argument(InputValue::new(
        "orderBy",
        TypeRef::named_list(order_by_type_name),
    ))
    .argument(InputValue::new("first", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("offset", TypeRef::named(TypeRef::INT)));

    GeneratedQuery {
        query_field,
        condition_type,
        condition_filter_types,
        order_by_enum,
        connection_type,
        edge_type,
    }
}

#[inline]
fn gql_err(msg: impl std::fmt::Display) -> async_graphql::Error {
    async_graphql::Error::new(msg.to_string())
}
