use async_graphql::Value as GqlValue;
use async_graphql::dynamic::{FieldValue, TypeRef};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use tokio_postgres::types::Type;

use crate::models::table::Column;

use super::sql_scalar::SqlScalar;

pub(crate) fn get_field_value<'a>(
    column: &Column,
    value: &serde_json::Value,
) -> Option<FieldValue<'a>> {
    let raw_val = value.get(column.name())?;

    if raw_val.is_null() {
        return None;
    }

    let field_val = match *column._type() {
        Type::BOOL => FieldValue::value(raw_val.as_bool()),
        Type::INT2 | Type::INT4 => FieldValue::value(raw_val.as_i64().map(|v| v as i32)),
        // i64 exceeds GraphQL Int (i32), so serialise as String
        Type::INT8 => FieldValue::value(raw_val.as_i64().map(|v| v.to_string())),
        Type::FLOAT4 | Type::FLOAT8 => FieldValue::value(raw_val.as_f64()),
        Type::NUMERIC => FieldValue::value(raw_val.as_f64()),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR => FieldValue::value(raw_val.as_str()),
        // JSON/JSONB: serialise to a JSON string
        Type::JSON | Type::JSONB => FieldValue::value(Some(raw_val.to_string())),
        // date/time: already serialised as ISO 8601 strings by Postgres row JSON
        Type::DATE | Type::TIME | Type::TIMETZ | Type::TIMESTAMP | Type::TIMESTAMPTZ => {
            FieldValue::value(raw_val.as_str())
        }
        // --- array types ---
        Type::BOOL_ARRAY => FieldValue::list(
            raw_val
                .as_array()
                .into_iter()
                .flatten()
                .map(|v| FieldValue::value(v.as_bool()))
                .collect::<Vec<_>>(),
        ),
        Type::INT2_ARRAY | Type::INT4_ARRAY => FieldValue::list(
            raw_val
                .as_array()
                .into_iter()
                .flatten()
                .map(|v| FieldValue::value(v.as_i64().map(|n| n as i32)))
                .collect::<Vec<_>>(),
        ),
        Type::INT8_ARRAY => FieldValue::list(
            raw_val
                .as_array()
                .into_iter()
                .flatten()
                .map(|v| FieldValue::value(v.as_i64().map(|n| n.to_string())))
                .collect::<Vec<_>>(),
        ),
        Type::FLOAT4_ARRAY | Type::FLOAT8_ARRAY => FieldValue::list(
            raw_val
                .as_array()
                .into_iter()
                .flatten()
                .map(|v| FieldValue::value(v.as_f64()))
                .collect::<Vec<_>>(),
        ),
        Type::TEXT_ARRAY | Type::VARCHAR_ARRAY | Type::BPCHAR_ARRAY => FieldValue::list(
            raw_val
                .as_array()
                .into_iter()
                .flatten()
                .map(|v| FieldValue::value(v.as_str()))
                .collect::<Vec<_>>(),
        ),
        Type::JSON_ARRAY | Type::JSONB_ARRAY => FieldValue::list(
            raw_val
                .as_array()
                .into_iter()
                .flatten()
                .map(|v| FieldValue::value(Some(v.to_string())))
                .collect::<Vec<_>>(),
        ),
        _ => FieldValue::value(raw_val.as_str()),
    };

    Some(field_val)
}

pub(crate) fn get_type_ref(column: &Column) -> TypeRef {
    let (base, is_list): (&str, bool) = match *column._type() {
        Type::BOOL => (TypeRef::BOOLEAN, false),
        Type::INT2 | Type::INT4 => (TypeRef::INT, false),
        // i64 exceeds GraphQL Int (i32), expose as String
        Type::INT8 => (TypeRef::STRING, false),
        Type::FLOAT4 | Type::FLOAT8 => (TypeRef::FLOAT, false),
        Type::NUMERIC => (TypeRef::FLOAT, false),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR => (TypeRef::STRING, false),
        // JSON/JSONB serialised as a JSON string
        Type::JSON | Type::JSONB => (TypeRef::STRING, false),
        // date/time types serialised as ISO 8601 strings
        Type::DATE | Type::TIME | Type::TIMETZ | Type::TIMESTAMP | Type::TIMESTAMPTZ => {
            (TypeRef::STRING, false)
        }
        // --- array types ---
        Type::BOOL_ARRAY => (TypeRef::BOOLEAN, true),
        Type::INT2_ARRAY | Type::INT4_ARRAY => (TypeRef::INT, true),
        Type::INT8_ARRAY => (TypeRef::STRING, true),
        Type::FLOAT4_ARRAY | Type::FLOAT8_ARRAY => (TypeRef::FLOAT, true),
        Type::TEXT_ARRAY | Type::VARCHAR_ARRAY | Type::BPCHAR_ARRAY => (TypeRef::STRING, true),
        Type::JSON_ARRAY | Type::JSONB_ARRAY => (TypeRef::STRING, true),
        _ => (TypeRef::STRING, false),
    };

    match (is_list, column.nullable()) {
        (false, true) => TypeRef::named(base),
        (false, false) => TypeRef::named_nn(base),
        (true, true) => TypeRef::named_list(base),
        (true, false) => TypeRef::named_nn_list(base),
    }
}

/// Returns a nullable scalar `TypeRef` for use in a condition input object.
/// Returns `None` for array / unsupported types (they cannot be equality-filtered).
pub(crate) fn condition_type_ref(column: &Column) -> Option<TypeRef> {
    let scalar = match *column._type() {
        Type::BOOL => TypeRef::BOOLEAN,
        Type::INT2 | Type::INT4 => TypeRef::INT,
        // INT8 mapped to String (i64 > i32 GraphQL range)
        Type::INT8 => TypeRef::STRING,
        Type::FLOAT4 | Type::FLOAT8 => TypeRef::FLOAT,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR => TypeRef::STRING,
        // JSON/JSONB accept a serialised JSON string for filtering
        Type::JSON | Type::JSONB => TypeRef::STRING,
        // NUMERIC: accept as Float for filtering
        Type::NUMERIC => TypeRef::FLOAT,
        // date/time: accept ISO 8601 strings for filtering
        Type::DATE | Type::TIME | Type::TIMESTAMP | Type::TIMESTAMPTZ => TypeRef::STRING,
        // arrays and everything else are excluded from condition
        _ => return None,
    };
    // Always nullable — every condition field is optional
    Some(TypeRef::named(scalar))
}

/// Converts an incoming GraphQL argument value to a typed SQL parameter.
pub(crate) fn to_sql_scalar(column: &Column, val: &GqlValue) -> Option<SqlScalar> {
    match *column._type() {
        Type::BOOL => {
            if let GqlValue::Boolean(b) = val {
                Some(SqlScalar::Bool(*b))
            } else {
                None
            }
        }
        Type::INT2 => {
            if let GqlValue::Number(n) = val {
                n.as_i64().map(|v| SqlScalar::Int2(v as i16))
            } else {
                None
            }
        }
        Type::INT4 => {
            if let GqlValue::Number(n) = val {
                n.as_i64().map(|v| SqlScalar::Int4(v as i32))
            } else {
                None
            }
        }
        // INT8 is exposed as String in the schema
        Type::INT8 => match val {
            GqlValue::Number(n) => n.as_i64().map(SqlScalar::Int8),
            GqlValue::String(s) => s.parse::<i64>().ok().map(SqlScalar::Int8),
            _ => None,
        },
        Type::FLOAT4 => {
            if let GqlValue::Number(n) = val {
                n.as_f64().map(|v| SqlScalar::Float4(v as f32))
            } else {
                None
            }
        }
        Type::FLOAT8 => {
            if let GqlValue::Number(n) = val {
                n.as_f64().map(SqlScalar::Float8)
            } else {
                None
            }
        }
        Type::TEXT | Type::VARCHAR | Type::BPCHAR => {
            if let GqlValue::String(s) = val {
                Some(SqlScalar::Text(s.clone()))
            } else {
                None
            }
        }
        // JSON/JSONB condition value is a serialised JSON string
        Type::JSON | Type::JSONB => {
            if let GqlValue::String(s) = val {
                serde_json::from_str(s).ok().map(SqlScalar::Json)
            } else {
                None
            }
        }
        Type::NUMERIC => {
            if let GqlValue::Number(n) = val {
                n.as_f64().map(SqlScalar::Numeric)
            } else {
                None
            }
        }
        Type::DATE => {
            if let GqlValue::String(s) = val {
                s.parse::<NaiveDate>().ok().map(SqlScalar::Date)
            } else {
                None
            }
        }
        Type::TIME => {
            if let GqlValue::String(s) = val {
                s.parse::<NaiveTime>().ok().map(SqlScalar::Time)
            } else {
                None
            }
        }
        Type::TIMESTAMP => {
            if let GqlValue::String(s) = val {
                s.parse::<NaiveDateTime>().ok().map(SqlScalar::Timestamp)
            } else {
                None
            }
        }
        Type::TIMESTAMPTZ => {
            if let GqlValue::String(s) = val {
                DateTime::parse_from_rfc3339(s)
                    .ok()
                    .map(|dt| SqlScalar::Timestamptz(dt.with_timezone(&Utc)))
            } else {
                None
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::sql_scalar::SqlScalar;
    use super::*;
    use crate::models::table::Column;
    use async_graphql::Value as GqlValue;
    use serde_json::json;
    use tokio_postgres::types::Type;

    // ── get_type_ref ─────────────────────────────────────────────────────────

    #[test]
    fn test_type_ref_bool_non_nullable() {
        let col = Column::new_for_test("active", Type::BOOL, false, false);
        assert_eq!(get_type_ref(&col).to_string(), "Boolean!");
    }

    #[test]
    fn test_type_ref_bool_nullable() {
        let col = Column::new_for_test("active", Type::BOOL, true, false);
        assert_eq!(get_type_ref(&col).to_string(), "Boolean");
    }

    #[test]
    fn test_type_ref_int4_non_nullable() {
        let col = Column::new_for_test("count", Type::INT4, false, false);
        assert_eq!(get_type_ref(&col).to_string(), "Int!");
    }

    #[test]
    fn test_type_ref_int4_nullable() {
        let col = Column::new_for_test("count", Type::INT4, true, false);
        assert_eq!(get_type_ref(&col).to_string(), "Int");
    }

    #[test]
    fn test_type_ref_int8_exposed_as_string() {
        let col = Column::new_for_test("big_id", Type::INT8, false, false);
        assert_eq!(get_type_ref(&col).to_string(), "String!");
    }

    #[test]
    fn test_type_ref_float4_non_nullable() {
        let col = Column::new_for_test("price", Type::FLOAT4, false, false);
        assert_eq!(get_type_ref(&col).to_string(), "Float!");
    }

    #[test]
    fn test_type_ref_float8_nullable() {
        let col = Column::new_for_test("price", Type::FLOAT8, true, false);
        assert_eq!(get_type_ref(&col).to_string(), "Float");
    }

    #[test]
    fn test_type_ref_text_non_nullable() {
        let col = Column::new_for_test("title", Type::TEXT, false, false);
        assert_eq!(get_type_ref(&col).to_string(), "String!");
    }

    #[test]
    fn test_type_ref_varchar_non_nullable() {
        let col = Column::new_for_test("code", Type::VARCHAR, false, false);
        assert_eq!(get_type_ref(&col).to_string(), "String!");
    }

    #[test]
    fn test_type_ref_jsonb_non_nullable() {
        let col = Column::new_for_test("meta", Type::JSONB, false, false);
        assert_eq!(get_type_ref(&col).to_string(), "String!");
    }

    #[test]
    fn test_type_ref_json_nullable() {
        let col = Column::new_for_test("meta", Type::JSON, true, false);
        assert_eq!(get_type_ref(&col).to_string(), "String");
    }

    #[test]
    fn test_type_ref_bool_array_non_nullable() {
        let col = Column::new_for_test("flags", Type::BOOL_ARRAY, false, false);
        assert_eq!(get_type_ref(&col).to_string(), "[Boolean!]");
    }

    #[test]
    fn test_type_ref_bool_array_nullable() {
        let col = Column::new_for_test("flags", Type::BOOL_ARRAY, true, false);
        assert_eq!(get_type_ref(&col).to_string(), "[Boolean]");
    }

    #[test]
    fn test_type_ref_int4_array_non_nullable() {
        let col = Column::new_for_test("ids", Type::INT4_ARRAY, false, false);
        assert_eq!(get_type_ref(&col).to_string(), "[Int!]");
    }

    #[test]
    fn test_type_ref_int4_array_nullable() {
        let col = Column::new_for_test("ids", Type::INT4_ARRAY, true, false);
        assert_eq!(get_type_ref(&col).to_string(), "[Int]");
    }

    #[test]
    fn test_type_ref_text_array_non_nullable() {
        let col = Column::new_for_test("tags", Type::TEXT_ARRAY, false, false);
        assert_eq!(get_type_ref(&col).to_string(), "[String!]");
    }

    #[test]
    fn test_type_ref_jsonb_array_non_nullable() {
        let col = Column::new_for_test("payloads", Type::JSONB_ARRAY, false, false);
        assert_eq!(get_type_ref(&col).to_string(), "[String!]");
    }

    // ── get_field_value ───────────────────────────────────────────────────────

    #[test]
    fn test_field_value_missing_key_returns_none() {
        let col = Column::new_for_test("name", Type::TEXT, false, false);
        let val = json!({ "other": "value" });
        assert!(get_field_value(&col, &val).is_none());
    }

    #[test]
    fn test_field_value_null_returns_none() {
        let col = Column::new_for_test("name", Type::TEXT, true, false);
        let val = json!({ "name": null });
        assert!(get_field_value(&col, &val).is_none());
    }

    #[test]
    fn test_field_value_bool_present() {
        let col = Column::new_for_test("active", Type::BOOL, false, false);
        let val = json!({ "active": true });
        assert!(get_field_value(&col, &val).is_some());
    }

    #[test]
    fn test_field_value_int2_present() {
        let col = Column::new_for_test("score", Type::INT2, false, false);
        let val = json!({ "score": 7 });
        assert!(get_field_value(&col, &val).is_some());
    }

    #[test]
    fn test_field_value_int4_present() {
        let col = Column::new_for_test("count", Type::INT4, false, false);
        let val = json!({ "count": 42 });
        assert!(get_field_value(&col, &val).is_some());
    }

    #[test]
    fn test_field_value_int8_present() {
        let col = Column::new_for_test("big_id", Type::INT8, false, false);
        let val = json!({ "big_id": 9223372036854775807_i64 });
        assert!(get_field_value(&col, &val).is_some());
    }

    #[test]
    fn test_field_value_float8_present() {
        let col = Column::new_for_test("price", Type::FLOAT8, false, false);
        let val = json!({ "price": 3.14 });
        assert!(get_field_value(&col, &val).is_some());
    }

    #[test]
    fn test_field_value_text_present() {
        let col = Column::new_for_test("title", Type::TEXT, false, false);
        let val = json!({ "title": "hello" });
        assert!(get_field_value(&col, &val).is_some());
    }

    #[test]
    fn test_field_value_jsonb_present() {
        let col = Column::new_for_test("meta", Type::JSONB, false, false);
        let val = json!({ "meta": { "key": "value" } });
        assert!(get_field_value(&col, &val).is_some());
    }

    #[test]
    fn test_field_value_bool_array_present() {
        let col = Column::new_for_test("flags", Type::BOOL_ARRAY, false, false);
        let val = json!({ "flags": [true, false, true] });
        assert!(get_field_value(&col, &val).is_some());
    }

    #[test]
    fn test_field_value_int4_array_present() {
        let col = Column::new_for_test("ids", Type::INT4_ARRAY, false, false);
        let val = json!({ "ids": [1, 2, 3] });
        assert!(get_field_value(&col, &val).is_some());
    }

    #[test]
    fn test_field_value_int8_array_present() {
        let col = Column::new_for_test("ids", Type::INT8_ARRAY, false, false);
        let val = json!({ "ids": [1000000000000_i64, 2000000000000_i64] });
        assert!(get_field_value(&col, &val).is_some());
    }

    #[test]
    fn test_field_value_float8_array_present() {
        let col = Column::new_for_test("scores", Type::FLOAT8_ARRAY, false, false);
        let val = json!({ "scores": [1.1, 2.2] });
        assert!(get_field_value(&col, &val).is_some());
    }

    #[test]
    fn test_field_value_text_array_present() {
        let col = Column::new_for_test("tags", Type::TEXT_ARRAY, false, false);
        let val = json!({ "tags": ["rust", "graphql"] });
        assert!(get_field_value(&col, &val).is_some());
    }

    #[test]
    fn test_field_value_jsonb_array_present() {
        let col = Column::new_for_test("payloads", Type::JSONB_ARRAY, false, false);
        let val = json!({ "payloads": [{"a": 1}, {"b": 2}] });
        assert!(get_field_value(&col, &val).is_some());
    }

    // ── condition_type_ref ───────────────────────────────────────────────────

    #[test]
    fn test_condition_type_ref_bool_nullable() {
        let col = Column::new_for_test("active", Type::BOOL, false, false);
        assert_eq!(condition_type_ref(&col).unwrap().to_string(), "Boolean");
    }

    #[test]
    fn test_condition_type_ref_int4_nullable() {
        let col = Column::new_for_test("count", Type::INT4, false, false);
        assert_eq!(condition_type_ref(&col).unwrap().to_string(), "Int");
    }

    #[test]
    fn test_condition_type_ref_int8_as_string() {
        let col = Column::new_for_test("big_id", Type::INT8, false, false);
        assert_eq!(condition_type_ref(&col).unwrap().to_string(), "String");
    }

    #[test]
    fn test_condition_type_ref_text_nullable() {
        let col = Column::new_for_test("name", Type::TEXT, false, false);
        assert_eq!(condition_type_ref(&col).unwrap().to_string(), "String");
    }

    #[test]
    fn test_condition_type_ref_jsonb_nullable() {
        let col = Column::new_for_test("meta", Type::JSONB, false, false);
        assert_eq!(condition_type_ref(&col).unwrap().to_string(), "String");
    }

    #[test]
    fn test_condition_type_ref_array_excluded() {
        let col = Column::new_for_test("ids", Type::INT4_ARRAY, false, false);
        assert!(condition_type_ref(&col).is_none());
    }

    #[test]
    fn test_condition_type_ref_bool_array_excluded() {
        let col = Column::new_for_test("flags", Type::BOOL_ARRAY, false, false);
        assert!(condition_type_ref(&col).is_none());
    }

    // ── to_sql_scalar ────────────────────────────────────────────────────────

    #[test]
    fn test_to_sql_scalar_bool() {
        let col = Column::new_for_test("active", Type::BOOL, false, false);
        assert!(matches!(
            to_sql_scalar(&col, &GqlValue::Boolean(true)),
            Some(SqlScalar::Bool(true))
        ));
    }

    #[test]
    fn test_to_sql_scalar_int4() {
        let col = Column::new_for_test("count", Type::INT4, false, false);
        let val = GqlValue::Number(serde_json::Number::from(42_i64));
        assert!(matches!(
            to_sql_scalar(&col, &val),
            Some(SqlScalar::Int4(42))
        ));
    }

    #[test]
    fn test_to_sql_scalar_int8_from_string() {
        let col = Column::new_for_test("big_id", Type::INT8, false, false);
        let val = GqlValue::String("9223372036854775807".to_string());
        assert!(matches!(
            to_sql_scalar(&col, &val),
            Some(SqlScalar::Int8(9223372036854775807))
        ));
    }

    #[test]
    fn test_to_sql_scalar_text() {
        let col = Column::new_for_test("name", Type::TEXT, false, false);
        let val = GqlValue::String("alice".to_string());
        assert!(matches!(
            to_sql_scalar(&col, &val),
            Some(SqlScalar::Text(_))
        ));
    }

    #[test]
    fn test_to_sql_scalar_wrong_type_returns_none() {
        let col = Column::new_for_test("active", Type::BOOL, false, false);
        let val = GqlValue::String("true".to_string());
        assert!(to_sql_scalar(&col, &val).is_none());
    }

    #[test]
    fn test_to_sql_scalar_array_col_returns_none() {
        let col = Column::new_for_test("ids", Type::INT4_ARRAY, false, false);
        let val = GqlValue::Number(serde_json::Number::from(1_i64));
        assert!(to_sql_scalar(&col, &val).is_none());
    }
}
