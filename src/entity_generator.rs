use async_graphql::dynamic::{Field, FieldFuture, FieldValue, TypeRef};

use crate::schema::{Column, Table};

fn generate_field(column: &Column) {
    Field::new(column.name, TypeRef::named_nn(TypeRef::STRING), |ctx| {
        FieldFuture::new(async move {
            let picture = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
            Ok(Some(FieldValue::value(&picture.url)))
        })
    });
}

pub fn generate_entity(table: &Table) {}
