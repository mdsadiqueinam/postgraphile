use std::sync::Arc;

use crate::table::{Column, Table};
use crate::utils::inflection::{singularize, to_pascal_case};
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, TypeRef};
use tokio_postgres::types::Type;

fn get_field_value<'a>(column: &Column, value: &serde_json::Value) -> Option<FieldValue<'a>> {
    if let Some(raw_val) = value.get(column.name()) {
        let field_val = match *column._type() {
            Type::BOOL => {
                let typed_val = raw_val.as_bool();
                FieldValue::value(typed_val)
            }
            _ => {
                let typed_val = raw_val.as_str();
                FieldValue::value(typed_val)
            }
        };

        Some(field_val)
    } else {
        FieldValue::none()
    }
}

fn get_type_ref(column: &Column) -> TypeRef {
    let base = match *column._type() {
        Type::BOOL => TypeRef::BOOLEAN,
        Type::INT2 | Type::INT4 => TypeRef::INT,
        Type::FLOAT4 | Type::FLOAT8 => TypeRef::FLOAT,
        _ => TypeRef::STRING,
    };
    if column.nullable() {
        TypeRef::named(base)
    } else {
        TypeRef::named_nn(base)
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
