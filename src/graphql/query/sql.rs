use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use async_graphql::Value as GqlValue;

use crate::models::table::Column;

use super::super::filter::{FilterOp, supports_range};
use super::super::sql_scalar::SqlScalar;
use super::super::type_mapping::to_sql_scalar;

pub(super) fn build_where_clause(
    sql: &mut String,
    params: &mut Vec<SqlScalar>,
    pairs: Vec<(String, GqlValue)>,
    columns: &[Arc<Column>],
    col_by_name: &HashMap<String, usize>,
) -> Result<(), async_graphql::Error> {
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
                    push_in_clause(sql, params, col, op_val, &mut has_where)?;
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
    Ok(())
}

fn push_in_clause(
    sql: &mut String,
    params: &mut Vec<SqlScalar>,
    col: &Column,
    op_val: GqlValue,
    has_where: &mut bool,
) -> Result<(), async_graphql::Error> {
    if let GqlValue::List(values) = op_val {
        if values.len() > 10_000 {
            return Err(super::gql_err("IN filter exceeds maximum of 10,000 items"));
        }
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
    Ok(())
}

pub(super) fn build_order_by_clause(
    sql: &mut String,
    order_by: &[String],
    columns: &[Arc<Column>],
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
            return Err(super::gql_err(format!(
                "unknown column for ordering: {col_upper}"
            )));
        };
        if i > 0 {
            sql.push_str(", ");
        }
        write!(sql, "\"{}\" {}", columns[col_idx].name(), dir).unwrap();
    }
    Ok(())
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
