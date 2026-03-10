use std::sync::Arc;

use crate::extensions::JsonListExt;
use crate::table::{Column, Table};
use crate::utils::inflection::{singularize, to_camel_case, to_pascal_case};
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, TypeRef};
use deadpool_postgres::Pool;
use tokio_postgres::types::Type;

fn get_field_value<'a>(column: &Column, value: &serde_json::Value) -> Option<FieldValue<'a>> {
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
        Type::TEXT | Type::VARCHAR | Type::BPCHAR => FieldValue::value(raw_val.as_str()),
        // JSON/JSONB: serialise to a JSON string
        Type::JSON | Type::JSONB => FieldValue::value(Some(raw_val.to_string())),
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

fn get_type_ref(column: &Column) -> TypeRef {
    let (base, is_list): (&str, bool) = match *column._type() {
        Type::BOOL => (TypeRef::BOOLEAN, false),
        Type::INT2 | Type::INT4 => (TypeRef::INT, false),
        // i64 exceeds GraphQL Int (i32), expose as String
        Type::INT8 => (TypeRef::STRING, false),
        Type::FLOAT4 | Type::FLOAT8 => (TypeRef::FLOAT, false),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR => (TypeRef::STRING, false),
        // JSON/JSONB serialised as a JSON string
        Type::JSON | Type::JSONB => (TypeRef::STRING, false),
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

fn generate_field(column: Arc<Column>) -> Field {
    Field::new(
        column.name().to_string(),
        get_type_ref(&column),
        move |ctx| {
            let column = column.clone();

            FieldFuture::new(async move {
                let parent_value = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
                let field_value = get_field_value(&column, parent_value);
                Ok(field_value)
            })
        },
    )
}

pub fn generate_entity(table: Arc<Table>) -> Object {
    let type_name = to_pascal_case(&singularize(table.name()));
    let obj = Object::new(type_name.as_str());

    table
        .columns()
        .iter()
        .filter(|col| !col.omit_read())
        .fold(obj, |obj, col| {
            obj.field(generate_field(Arc::new(col.clone())))
        })
}

/// Generates a root Query field (e.g. `allUsers`) that fetches every row from
/// the backing table and returns them as a list of the entity type.  Each row
/// is converted to a `serde_json::Value` so the entity field resolvers can
/// extract individual column values via `get_field_value`.
pub fn generate_query_field(table: Arc<Table>, pool: Arc<Pool>) -> Field {
    let type_name = to_pascal_case(&singularize(table.name()));
    // field name: allBlogPosts, allUsers, …
    let field_name = format!("all{}", to_pascal_case(table.name()));
    let schema = table.schema_name().to_string();
    let tbl = table.name().to_string();

    Field::new(
        field_name,
        TypeRef::named_nn_list_nn(type_name),
        move |_ctx| {
            let pool = pool.clone();
            let schema = schema.clone();
            let tbl = tbl.clone();

            FieldFuture::new(async move {
                let client = pool
                    .get()
                    .await
                    .map_err(|e| async_graphql::Error::new(format!("DB pool error: {e}")))?;

                let sql = format!("SELECT * FROM \"{schema}\".\"{tbl}\"");
                let rows = client
                    .query(sql.as_str(), &[])
                    .await
                    .map_err(|e| async_graphql::Error::new(format!("DB query error: {e}")))?;

                let json_rows = rows.to_json_list();
                let values = json_rows
                    .into_iter()
                    .map(|row| FieldValue::owned_any(row))
                    .collect::<Vec<_>>();

                Ok(Some(FieldValue::list(values)))
            })
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{Column, Table};
    use serde_json::json;

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
        // INT8 (i64) exceeds GraphQL Int (i32) range, so it is mapped to String
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

    // array types — nullable column → named_list (nullable list, nullable elements)
    //             — non-nullable column → named_nn_list ([T!], nullable list of non-null elements)
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

    // ── generate_entity ───────────────────────────────────────────────────────

    #[test]
    fn test_entity_name_singularized_and_pascal_cased() {
        let table = Arc::new(Table::new_for_test("blog_posts", vec![]));
        assert_eq!(generate_entity(table).type_name(), "BlogPost");
    }

    #[test]
    fn test_entity_name_already_singular() {
        let table = Arc::new(Table::new_for_test("users", vec![]));
        assert_eq!(generate_entity(table).type_name(), "User");
    }

    #[test]
    fn test_entity_name_single_word() {
        let table = Arc::new(Table::new_for_test("orders", vec![]));
        assert_eq!(generate_entity(table).type_name(), "Order");
    }

    #[test]
    fn test_entity_omit_read_column_excluded() {
        // Two columns share the same name: one visible, one @omit read.
        // Object::field() panics on duplicate names, so if the omitted column
        // were included this test would panic.
        let visible = Column::new_for_test("secret", Type::TEXT, false, false);
        let hidden = Column::new_for_test("secret", Type::TEXT, false, true);
        let table = Arc::new(Table::new_for_test("users", vec![visible, hidden]));
        generate_entity(table); // panics if hidden was not filtered out
    }

    #[test]
    fn test_entity_no_columns_empty_object() {
        let table = Arc::new(Table::new_for_test("tokens", vec![]));
        let obj = generate_entity(table);
        assert_eq!(obj.type_name(), "Token");
    }
}
