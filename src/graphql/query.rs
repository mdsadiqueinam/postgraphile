use std::sync::Arc;

use async_graphql::Value as GqlValue;
use async_graphql::dynamic::{
    Enum, EnumItem, Field, FieldFuture, FieldValue, InputObject, InputValue, TypeRef,
};
use deadpool_postgres::Pool;
use tokio_postgres::types::{ToSql, Type};

use crate::db::JsonListExt;
use crate::table::Table;
use crate::utils::inflection::to_pascal_case;

use super::sql_scalar::SqlScalar;
use super::type_mapping::{condition_type_ref, to_sql_scalar};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FilterOp {
    Eq,
    NotEqual,
    In,
    Gt,
    Gte,
    Lt,
    Lte,
}

fn parse_filter_op_key(key: &str) -> Option<FilterOp> {
    match key {
        "equal" => Some(FilterOp::Eq),
        "notEqual" => Some(FilterOp::NotEqual),
        "in" => Some(FilterOp::In),
        "greaterThan" => Some(FilterOp::Gt),
        "greaterThanEqual" => Some(FilterOp::Gte),
        "lessThan" => Some(FilterOp::Lt),
        "lessThanEqual" => Some(FilterOp::Lte),
        _ => None,
    }
}

fn supports_range(column_type: &Type) -> bool {
    matches!(
        *column_type,
        Type::INT2 | Type::INT4 | Type::INT8 | Type::FLOAT4 | Type::FLOAT8
    )
}

/// Builds the `{TypeName}Condition` input object (equality filters per column).
/// Exported so callers can register it with the schema separately.
pub fn make_condition_filter_types(table: &Table) -> Vec<InputObject> {
    table
        .columns()
        .iter()
        .filter(|c| !c.omit_read())
        .filter_map(|col| {
            condition_type_ref(col).map(|tr| {
                let scalar_name = tr.to_string();
                let filter_name =
                    format!("{}{}Filter", table.type_name(), to_pascal_case(col.name()));
                let mut input = InputObject::new(filter_name)
                    .field(InputValue::new("equal", tr.clone()))
                    .field(InputValue::new("notEqual", tr.clone()))
                    .field(InputValue::new("in", TypeRef::named_list(scalar_name)));

                if supports_range(col._type()) {
                    input = input
                        .field(InputValue::new("greaterThan", tr.clone()))
                        .field(InputValue::new("greaterThanEqual", tr.clone()))
                        .field(InputValue::new("lessThan", tr.clone()))
                        .field(InputValue::new("lessThen", tr.clone()))
                        .field(InputValue::new("lessThanEqual", tr.clone()))
                        .field(InputValue::new("lessThenEqual", tr));
                }

                input
            })
        })
        .collect()
}

/// Builds the `{TypeName}Condition` input object (per-column operator filters).
/// Exported so callers can register it with the schema separately.
pub fn make_condition_type(table: &Table) -> InputObject {
    let name = format!("{}Condition", table.type_name());
    let filter_types = make_condition_filter_types(table);

    table
        .columns()
        .iter()
        .filter(|c| !c.omit_read())
        .fold(
            InputObject::new(name),
            |obj, col| match condition_type_ref(col) {
                Some(_) => {
                    let filter_name =
                        format!("{}{}Filter", table.type_name(), to_pascal_case(col.name()));
                    if filter_types.iter().any(|f| f.type_name() == filter_name) {
                        obj.field(InputValue::new(
                            col.name().as_str(),
                            TypeRef::named(filter_name),
                        ))
                    } else {
                        obj
                    }
                }
                None => obj,
            },
        )
}

/// Builds the `{TypeName}OrderBy` enum (COLUMN_ASC / COLUMN_DESC per column).
/// Exported so callers can register it with the schema separately.
pub fn make_order_by_enum(table: &Table) -> Enum {
    let name = format!("{}OrderBy", table.type_name());
    table
        .columns()
        .iter()
        .filter(|c| !c.omit_read())
        .flat_map(|c| {
            let upper = c.name().to_uppercase();
            [
                EnumItem::new(format!("{}_ASC", upper)),
                EnumItem::new(format!("{}_DESC", upper)),
            ]
        })
        .fold(Enum::new(name), |e, item| e.item(item))
}

/// Everything the schema builder needs for one table.
pub struct GeneratedQuery {
    /// The root Query field (e.g. `allUsers`).
    pub query_field: Field,
    /// The `{T}Condition` input type — must be registered with the schema.
    pub condition_type: InputObject,
    /// Per-column filter input objects referenced by `{T}Condition`.
    pub condition_filter_types: Vec<InputObject>,
    /// The `{T}OrderBy` enum — must be registered with the schema.
    pub order_by_enum: Enum,
}

/// Generates a root Query field (e.g. `allUsers`) with PostGraphile-style
/// filtering arguments:
///
/// ```graphql
/// allUsers(
///   condition: UserCondition   # equality filter per column
///   orderBy:   UserOrderBy     # COLUMN_ASC / COLUMN_DESC
///   first:     Int             # LIMIT
///   offset:    Int             # OFFSET
/// ): [User!]!
/// ```
pub fn generate_query(table: Arc<Table>, pool: Arc<Pool>) -> GeneratedQuery {
    let condition_filter_types = make_condition_filter_types(&table);
    let condition_type = make_condition_type(&table);
    let order_by_enum = make_order_by_enum(&table);

    let type_name = table.type_name();
    let condition_type_name = condition_type.type_name().to_string();
    let order_by_type_name = order_by_enum.type_name().to_string();
    let field_name = format!("all{}", to_pascal_case(table.name()));
    let tbl_schema = table.schema_name().to_string();
    let tbl_name = table.name().to_string();
    let columns = Arc::new(table.columns().to_vec());

    let query_field = Field::new(
        field_name,
        TypeRef::named_nn_list_nn(type_name),
        move |ctx| {
            // ── extract args synchronously (ctx lifetime doesn't cross await) ──
            let condition_pairs: Option<Vec<(String, GqlValue)>> = ctx
                .args
                .get("condition")
                .and_then(|v| v.object().ok())
                .map(|obj| {
                    obj.iter()
                        .map(|(k, v)| (k.to_string(), v.as_value().clone()))
                        .collect()
                });

            let order_by = ctx
                .args
                .get("orderBy")
                .and_then(|v| v.enum_name().ok().map(|s| s.to_string()));

            let first = ctx.args.get("first").and_then(|v| v.i64().ok());
            let offset = ctx.args.get("offset").and_then(|v| v.i64().ok());

            let pool = pool.clone();
            let tbl_schema = tbl_schema.clone();
            let tbl_name = tbl_name.clone();
            let columns = columns.clone();

            FieldFuture::new(async move {
                // ── WHERE ────────────────────────────────────────────────────
                let mut where_clauses = Vec::<String>::new();
                let mut params = Vec::<SqlScalar>::new();

                if let Some(pairs) = condition_pairs {
                    for (key, gql_val) in pairs {
                        let Some(col) = columns
                            .iter()
                            .find(|c| !c.omit_read() && c.name() == key.as_str())
                        else {
                            continue;
                        };

                        // Backward-compatible shortcut: scalar means equality.
                        if !matches!(gql_val, GqlValue::Object(_)) {
                            if let Some(scalar) = to_sql_scalar(col, &gql_val) {
                                where_clauses.push(format!(
                                    "\"{}\" = ${}",
                                    col.name(),
                                    params.len() + 1
                                ));
                                params.push(scalar);
                            }
                            continue;
                        }

                        if let GqlValue::Object(op_obj) = gql_val {
                            for (op_key, op_val) in op_obj {
                                let Some(op) = parse_filter_op_key(op_key.as_str()) else {
                                    continue;
                                };

                                match op {
                                    FilterOp::Eq
                                    | FilterOp::NotEqual
                                    | FilterOp::Gt
                                    | FilterOp::Gte
                                    | FilterOp::Lt
                                    | FilterOp::Lte => {
                                        if matches!(
                                            op,
                                            FilterOp::Gt
                                                | FilterOp::Gte
                                                | FilterOp::Lt
                                                | FilterOp::Lte
                                        ) && !supports_range(col._type())
                                        {
                                            continue;
                                        }

                                        if let Some(scalar) = to_sql_scalar(col, &op_val) {
                                            let sql_op = match op {
                                                FilterOp::Eq => "=",
                                                FilterOp::NotEqual => "<>",
                                                FilterOp::Gt => ">",
                                                FilterOp::Gte => ">=",
                                                FilterOp::Lt => "<",
                                                FilterOp::Lte => "<=",
                                                FilterOp::In => unreachable!(),
                                            };
                                            where_clauses.push(format!(
                                                "\"{}\" {} ${}",
                                                col.name(),
                                                sql_op,
                                                params.len() + 1
                                            ));
                                            params.push(scalar);
                                        }
                                    }
                                    FilterOp::In => {
                                        if let GqlValue::List(values) = op_val {
                                            let mut local_placeholders = Vec::<String>::new();
                                            for val in values {
                                                if let Some(scalar) = to_sql_scalar(col, &val) {
                                                    local_placeholders
                                                        .push(format!("${}", params.len() + 1));
                                                    params.push(scalar);
                                                }
                                            }

                                            if !local_placeholders.is_empty() {
                                                where_clauses.push(format!(
                                                    "\"{}\" IN ({})",
                                                    col.name(),
                                                    local_placeholders.join(", ")
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                let where_sql = if where_clauses.is_empty() {
                    String::new()
                } else {
                    format!("WHERE {}", where_clauses.join(" AND "))
                };

                // ── ORDER BY ─────────────────────────────────────────────────
                // The value is one of our own enum variants (e.g. CREATED_AT_DESC),
                // so we validate the column name against the known column list
                // before interpolating — no injection possible.
                let order_sql = if let Some(s) = &order_by {
                    let (col_upper, dir) = if s.ends_with("_DESC") {
                        (&s[..s.len() - 5], "DESC")
                    } else {
                        (&s[..s.len() - 4], "ASC")
                    };
                    let col_name = col_upper.to_lowercase();
                    if columns
                        .iter()
                        .any(|c| !c.omit_read() && c.name() == col_name.as_str())
                    {
                        format!("ORDER BY \"{}\" {}", col_name, dir)
                    } else {
                        return Err(async_graphql::Error::new(format!(
                            "unknown column for ordering: {}",
                            col_name
                        )));
                    }
                } else {
                    String::new()
                };

                // ── LIMIT / OFFSET ───────────────────────────────────────────
                let limit_sql = first.map(|n| format!("LIMIT {}", n)).unwrap_or_default();
                let offset_sql = offset.map(|n| format!("OFFSET {}", n)).unwrap_or_default();

                // ── execute ──────────────────────────────────────────────────
                let sql = format!(
                    "SELECT * FROM \"{}\".\"{}\" {} {} {} {}",
                    tbl_schema, tbl_name, where_sql, order_sql, limit_sql, offset_sql
                );

                let param_refs: Vec<&(dyn ToSql + Sync)> =
                    params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

                let client = pool
                    .get()
                    .await
                    .map_err(|e| async_graphql::Error::new(format!("DB pool error: {e}")))?;

                let rows = client
                    .query(sql.as_str(), param_refs.as_slice())
                    .await
                    .map_err(|e| async_graphql::Error::new(format!("DB query error: {e}")))?;

                let values = rows
                    .to_json_list()
                    .into_iter()
                    .map(FieldValue::owned_any)
                    .collect::<Vec<_>>();

                Ok(Some(FieldValue::list(values)))
            })
        },
    )
    .argument(InputValue::new(
        "condition",
        TypeRef::named(condition_type_name),
    ))
    .argument(InputValue::new(
        "orderBy",
        TypeRef::named(order_by_type_name),
    ))
    .argument(InputValue::new("first", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("offset", TypeRef::named(TypeRef::INT)));

    GeneratedQuery {
        query_field,
        condition_type,
        condition_filter_types,
        order_by_enum,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::Table;
    use tokio_postgres::types::Type;

    #[test]
    fn test_condition_type_name() {
        let table = Table::new_for_test("blog_posts", vec![]);
        assert_eq!(make_condition_type(&table).type_name(), "BlogPostCondition");
    }

    #[test]
    fn test_condition_type_name_users() {
        let table = Table::new_for_test("users", vec![]);
        assert_eq!(make_condition_type(&table).type_name(), "UserCondition");
    }

    #[test]
    fn test_order_by_enum_name() {
        let table = Table::new_for_test("blog_posts", vec![]);
        assert_eq!(make_order_by_enum(&table).type_name(), "BlogPostOrderBy");
    }

    #[test]
    fn test_order_by_enum_name_users() {
        let table = Table::new_for_test("users", vec![]);
        assert_eq!(make_order_by_enum(&table).type_name(), "UserOrderBy");
    }

    #[test]
    fn test_parse_filter_op_key_not_equal() {
        assert_eq!(parse_filter_op_key("notEqual"), Some(FilterOp::NotEqual));
    }

    #[test]
    fn test_parse_filter_op_key_range() {
        assert_eq!(parse_filter_op_key("greaterThanEqual"), Some(FilterOp::Gte));
        assert_eq!(parse_filter_op_key("lessThan"), Some(FilterOp::Lt));
    }

    #[test]
    fn test_parse_filter_op_key_less_then_alias() {
        assert_eq!(parse_filter_op_key("lessThen"), Some(FilterOp::Lt));
        assert_eq!(parse_filter_op_key("lessThenEqual"), Some(FilterOp::Lte));
    }

    #[test]
    fn test_parse_filter_op_key_default_eq() {
        assert_eq!(parse_filter_op_key("equal"), Some(FilterOp::Eq));
    }

    #[test]
    fn test_parse_filter_op_key_unknown() {
        assert_eq!(parse_filter_op_key("between"), None);
    }

    #[test]
    fn test_supports_range_for_numeric() {
        assert!(supports_range(&Type::INT4));
        assert!(supports_range(&Type::FLOAT8));
    }

    #[test]
    fn test_supports_range_for_non_numeric() {
        assert!(!supports_range(&Type::TEXT));
        assert!(!supports_range(&Type::BOOL));
    }
}
