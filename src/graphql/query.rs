use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use async_graphql::Value as GqlValue;
use async_graphql::dynamic::{
    Enum, EnumItem, Field, FieldFuture, FieldValue, InputObject, InputValue, TypeRef,
};
use deadpool_postgres::Pool;
use tokio_postgres::types::{ToSql, Type};

use crate::db::JsonListExt;
use crate::table::{Column, Table};
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

impl FilterOp {
    fn from_key(key: &str) -> Option<Self> {
        match key {
            "equal" => Some(Self::Eq),
            "notEqual" => Some(Self::NotEqual),
            "in" => Some(Self::In),
            "greaterThan" => Some(Self::Gt),
            "greaterThanEqual" => Some(Self::Gte),
            "lessThan" => Some(Self::Lt),
            "lessThanEqual" => Some(Self::Lte),
            _ => None,
        }
    }

    fn sql_operator(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEqual => "<>",
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::Lt => "<",
            Self::Lte => "<=",
            Self::In => unreachable!("IN is not a simple binary operator"),
        }
    }

    fn is_range(self) -> bool {
        matches!(self, Self::Gt | Self::Gte | Self::Lt | Self::Lte)
    }
}

fn supports_range(column_type: &Type) -> bool {
    matches!(
        *column_type,
        Type::INT2
            | Type::INT4
            | Type::INT8
            | Type::FLOAT4
            | Type::FLOAT8
            | Type::NUMERIC
            | Type::DATE
            | Type::TIME
            | Type::TIMESTAMP
            | Type::TIMESTAMPTZ
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
                        .field(InputValue::new("lessThanEqual", tr));
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

    table
        .columns()
        .iter()
        .filter(|c| !c.omit_read())
        .fold(InputObject::new(name), |obj, col| {
            if condition_type_ref(col).is_some() {
                let filter_name =
                    format!("{}{}Filter", table.type_name(), to_pascal_case(col.name()));
                obj.field(InputValue::new(
                    col.name().as_str(),
                    TypeRef::named(filter_name),
                ))
            } else {
                obj
            }
        })
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
///   orderBy:   UserOrderBy     # [COLUMN_ASC] / [COLUMN_DESC]
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
    let col_by_name: Arc<HashMap<String, usize>> = Arc::new(
        columns
            .iter()
            .enumerate()
            .filter(|(_, c)| !c.omit_read())
            .map(|(i, c)| (c.name().to_string(), i))
            .collect(),
    );
    let col_by_upper: Arc<HashMap<String, usize>> = Arc::new(
        columns
            .iter()
            .enumerate()
            .filter(|(_, c)| !c.omit_read())
            .map(|(i, c)| (c.name().to_uppercase(), i))
            .collect(),
    );

    let query_field = Field::new(
        field_name,
        TypeRef::named_nn_list_nn(type_name),
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

            FieldFuture::new(async move {
                let mut sql = format!("SELECT * FROM \"{}\".\"{}\"", tbl_schema, tbl_name);
                let mut params = Vec::<SqlScalar>::with_capacity(8);

                if let Some(pairs) = condition_pairs {
                    build_where_clause(&mut sql, &mut params, pairs, &columns, &col_by_name);
                }
                build_order_by_clause(&mut sql, &order_by, &columns, &col_by_upper)?;
                if let Some(n) = first {
                    write!(sql, " LIMIT {}", n).unwrap();
                }
                if let Some(n) = offset {
                    write!(sql, " OFFSET {}", n).unwrap();
                }

                execute_query(&pool, &sql, &params).await
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
    }
}

// ── Extracted helpers ────────────────────────────────────────────────────────

fn build_where_clause(
    sql: &mut String,
    params: &mut Vec<SqlScalar>,
    pairs: Vec<(String, GqlValue)>,
    columns: &[Column],
    col_by_name: &HashMap<String, usize>,
) {
    let mut has_where = false;

    for (key, gql_val) in pairs {
        let Some(&col_idx) = col_by_name.get(&key) else {
            continue;
        };
        let col = &columns[col_idx];

        if !matches!(gql_val, GqlValue::Object(_)) {
            if let Some(scalar) = to_sql_scalar(col, &gql_val) {
                write_where_sep(sql, &mut has_where);
                write!(sql, "\"{}\" = ${}", col.name(), params.len() + 1).unwrap();
                params.push(scalar);
            }
            continue;
        }

        if let GqlValue::Object(op_obj) = gql_val {
            for (op_key, op_val) in op_obj {
                let Some(op) = FilterOp::from_key(op_key.as_str()) else {
                    continue;
                };

                if op == FilterOp::In {
                    push_in_clause(sql, params, col, op_val, &mut has_where);
                    continue;
                }

                if op.is_range() && !supports_range(col._type()) {
                    continue;
                }

                if let Some(scalar) = to_sql_scalar(col, &op_val) {
                    write_where_sep(sql, &mut has_where);
                    write!(
                        sql,
                        "\"{}\" {} ${}",
                        col.name(),
                        op.sql_operator(),
                        params.len() + 1
                    )
                    .unwrap();
                    params.push(scalar);
                }
            }
        }
    }
}

fn push_in_clause(
    sql: &mut String,
    params: &mut Vec<SqlScalar>,
    col: &Column,
    op_val: GqlValue,
    has_where: &mut bool,
) {
    if let GqlValue::List(values) = op_val {
        let scalars: Vec<SqlScalar> = values
            .into_iter()
            .filter_map(|val| to_sql_scalar(col, &val))
            .collect();

        if !scalars.is_empty() {
            write_where_sep(sql, has_where);
            let start = params.len() + 1;
            write!(sql, "\"{}\" IN (", col.name()).unwrap();
            for (i, scalar) in scalars.into_iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                write!(sql, "${}", start + i).unwrap();
                params.push(scalar);
            }
            sql.push(')');
        }
    }
}

fn build_order_by_clause(
    sql: &mut String,
    order_by: &[String],
    columns: &[Column],
    col_by_upper: &HashMap<String, usize>,
) -> Result<(), async_graphql::Error> {
    if order_by.is_empty() {
        return Ok(());
    }
    sql.push_str(" ORDER BY ");
    for (i, s) in order_by.iter().enumerate() {
        let (col_upper, dir) = if let Some(c) = s.strip_suffix("_DESC") {
            (c, "DESC")
        } else if let Some(c) = s.strip_suffix("_ASC") {
            (c, "ASC")
        } else {
            continue;
        };
        let Some(&col_idx) = col_by_upper.get(col_upper) else {
            return Err(async_graphql::Error::new(format!(
                "unknown column for ordering: {}",
                col_upper
            )));
        };
        if i > 0 {
            sql.push_str(", ");
        }
        write!(sql, "\"{}\" {}", columns[col_idx].name(), dir).unwrap();
    }
    Ok(())
}

async fn execute_query(
    pool: &Pool,
    sql: &str,
    params: &[SqlScalar],
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
    let param_refs: Vec<&(dyn ToSql + Sync)> =
        params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

    let client = pool
        .get()
        .await
        .map_err(|e| async_graphql::Error::new(format!("DB pool error: {e}")))?;

    let rows = client
        .query(sql, param_refs.as_slice())
        .await
        .map_err(|e| async_graphql::Error::new(format!("DB query error: {e}")))?;

    let values = rows
        .to_json_list()
        .into_iter()
        .map(FieldValue::owned_any)
        .collect::<Vec<_>>();

    Ok(Some(FieldValue::list(values)))
}

#[inline]
fn write_where_sep(sql: &mut String, has_where: &mut bool) {
    if *has_where {
        sql.push_str(" AND ");
    } else {
        sql.push_str(" WHERE ");
        *has_where = true;
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
    fn test_filter_op_from_key_not_equal() {
        assert_eq!(FilterOp::from_key("notEqual"), Some(FilterOp::NotEqual));
    }

    #[test]
    fn test_filter_op_from_key_range() {
        assert_eq!(FilterOp::from_key("greaterThanEqual"), Some(FilterOp::Gte));
        assert_eq!(FilterOp::from_key("lessThan"), Some(FilterOp::Lt));
    }

    #[test]
    fn test_filter_op_from_key_default_eq() {
        assert_eq!(FilterOp::from_key("equal"), Some(FilterOp::Eq));
    }

    #[test]
    fn test_filter_op_from_key_unknown() {
        assert_eq!(FilterOp::from_key("between"), None);
    }

    #[test]
    fn test_filter_op_sql_operator() {
        assert_eq!(FilterOp::Eq.sql_operator(), "=");
        assert_eq!(FilterOp::NotEqual.sql_operator(), "<>");
        assert_eq!(FilterOp::Gt.sql_operator(), ">");
        assert_eq!(FilterOp::Gte.sql_operator(), ">=");
        assert_eq!(FilterOp::Lt.sql_operator(), "<");
        assert_eq!(FilterOp::Lte.sql_operator(), "<=");
    }

    #[test]
    fn test_filter_op_is_range() {
        assert!(!FilterOp::Eq.is_range());
        assert!(!FilterOp::NotEqual.is_range());
        assert!(!FilterOp::In.is_range());
        assert!(FilterOp::Gt.is_range());
        assert!(FilterOp::Gte.is_range());
        assert!(FilterOp::Lt.is_range());
        assert!(FilterOp::Lte.is_range());
    }

    #[test]
    fn test_supports_range_for_numeric() {
        assert!(supports_range(&Type::INT2));
        assert!(supports_range(&Type::INT4));
        assert!(supports_range(&Type::INT8));
        assert!(supports_range(&Type::FLOAT4));
        assert!(supports_range(&Type::FLOAT8));
        assert!(supports_range(&Type::NUMERIC));
    }

    #[test]
    fn test_supports_range_for_datetime() {
        assert!(supports_range(&Type::DATE));
        assert!(supports_range(&Type::TIME));
        assert!(supports_range(&Type::TIMESTAMP));
        assert!(supports_range(&Type::TIMESTAMPTZ));
        // TIMETZ is excluded — no simple ToSql mapping available
        assert!(!supports_range(&Type::TIMETZ));
    }

    #[test]
    fn test_supports_range_for_non_numeric() {
        assert!(!supports_range(&Type::TEXT));
        assert!(!supports_range(&Type::BOOL));
        assert!(!supports_range(&Type::JSON));
    }
}
