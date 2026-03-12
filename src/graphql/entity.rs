use std::sync::Arc;

use async_graphql::dynamic::{Field, FieldFuture, Object};

use crate::table::{Column, Table};

use super::type_mapping::{get_field_value, get_type_ref};

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
    let type_name = table.type_name();
    let obj = Object::new(type_name.as_str());

    table
        .columns()
        .iter()
        .filter(|col| !col.omit_read())
        .fold(obj, |obj, col| obj.field(generate_field(col.clone())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{Column, Table};
    use std::sync::Arc;
    use tokio_postgres::types::Type;

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
        let visible = Column::new_for_test("name", Type::TEXT, false, false);
        let hidden = Column::new_for_test("secret", Type::TEXT, false, true);
        let table = Arc::new(Table::new_for_test("users", vec![visible, hidden]));
        generate_entity(table);
    }

    #[test]
    fn test_entity_no_columns_empty_object() {
        let table = Arc::new(Table::new_for_test("tokens", vec![]));
        let obj = generate_entity(table);
        assert_eq!(obj.type_name(), "Token");
    }
}
