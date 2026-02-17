use std::any::Any;

use crate::table::{Column, Table};
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, TypeRef};
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

fn generate_field<'a>(column: &Column) {
    Field::new(column.name(), TypeRef::named_nn(TypeRef::STRING), |ctx| {
        FieldFuture::new(async move {
            // Ok(FieldValue::none())
            let parent_value = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
            let field_value = get_field_value(column, parent_value);
            Ok(field_value)
        })
    });
}

pub fn generate_entity(table: &Table) {}
