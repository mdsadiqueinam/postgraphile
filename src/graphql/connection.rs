use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, TypeRef};
use base64::Engine;

use crate::table::Table;

#[derive(Clone, Debug)]
pub struct EdgePayload {
    pub cursor: String,
    pub node: serde_json::Value,
}

#[derive(Clone, Debug)]
pub struct ConnectionPayload {
    pub total_count: i64,
    pub has_next_page: bool,
    pub has_previous_page: bool,
    pub edges: Vec<EdgePayload>,
}

pub fn encode_cursor(order_by: &[String], abs_index: usize) -> String {
    let json = if order_by.is_empty() {
        serde_json::json!([abs_index + 1])
    } else {
        let keys: Vec<String> = order_by.iter().map(|s| s.to_lowercase()).collect();
        serde_json::json!([keys, abs_index + 1])
    };
    base64::engine::general_purpose::STANDARD.encode(json.to_string())
}

// ── Shared PageInfo type (register once globally) ───────────────────────────

pub fn make_page_info_type() -> Object {
    Object::new("PageInfo")
        .field(Field::new(
            "hasNextPage",
            TypeRef::named_nn(TypeRef::BOOLEAN),
            |ctx| {
                FieldFuture::new(async move {
                    let payload = ctx.parent_value.try_downcast_ref::<ConnectionPayload>()?;
                    Ok(Some(FieldValue::value(payload.has_next_page)))
                })
            },
        ))
        .field(Field::new(
            "hasPreviousPage",
            TypeRef::named_nn(TypeRef::BOOLEAN),
            |ctx| {
                FieldFuture::new(async move {
                    let payload = ctx.parent_value.try_downcast_ref::<ConnectionPayload>()?;
                    Ok(Some(FieldValue::value(payload.has_previous_page)))
                })
            },
        ))
        .field(Field::new(
            "startCursor",
            TypeRef::named(TypeRef::STRING),
            |ctx| {
                FieldFuture::new(async move {
                    let payload = ctx.parent_value.try_downcast_ref::<ConnectionPayload>()?;
                    let val = payload
                        .edges
                        .first()
                        .map(|e| FieldValue::value(e.cursor.clone()));
                    Ok(val)
                })
            },
        ))
        .field(Field::new(
            "endCursor",
            TypeRef::named(TypeRef::STRING),
            |ctx| {
                FieldFuture::new(async move {
                    let payload = ctx.parent_value.try_downcast_ref::<ConnectionPayload>()?;
                    let val = payload
                        .edges
                        .last()
                        .map(|e| FieldValue::value(e.cursor.clone()));
                    Ok(val)
                })
            },
        ))
}

// ── Per-table Connection + Edge types ───────────────────────────────────────

pub fn make_connection_types(table: &Table) -> (Object, Object) {
    let type_name = table.type_name();
    let edge_type_name = format!("{}Edge", type_name);
    let connection_type_name = format!("{}Connection", type_name);

    let node_type = type_name.clone();
    let edge = Object::new(&edge_type_name)
        .field(Field::new(
            "cursor",
            TypeRef::named_nn(TypeRef::STRING),
            |ctx| {
                FieldFuture::new(async move {
                    let edge = ctx.parent_value.try_downcast_ref::<EdgePayload>()?;
                    Ok(Some(FieldValue::value(edge.cursor.clone())))
                })
            },
        ))
        .field(Field::new("node", TypeRef::named_nn(node_type), |ctx| {
            FieldFuture::new(async move {
                let edge = ctx.parent_value.try_downcast_ref::<EdgePayload>()?;
                Ok(Some(FieldValue::owned_any(edge.node.clone())))
            })
        }));

    let edge_ref = edge_type_name.clone();
    let connection = Object::new(&connection_type_name)
        .field(Field::new(
            "totalCount",
            TypeRef::named_nn(TypeRef::INT),
            |ctx| {
                FieldFuture::new(async move {
                    let payload = ctx.parent_value.try_downcast_ref::<ConnectionPayload>()?;
                    Ok(Some(FieldValue::value(payload.total_count as i32)))
                })
            },
        ))
        .field(Field::new(
            "pageInfo",
            TypeRef::named_nn("PageInfo"),
            |ctx| {
                FieldFuture::new(async move {
                    let payload = ctx.parent_value.try_downcast_ref::<ConnectionPayload>()?;
                    Ok(Some(FieldValue::owned_any(payload.clone())))
                })
            },
        ))
        .field(Field::new(
            "edges",
            TypeRef::named_nn_list_nn(edge_ref),
            |ctx| {
                FieldFuture::new(async move {
                    let payload = ctx.parent_value.try_downcast_ref::<ConnectionPayload>()?;
                    let list: Vec<FieldValue> = payload
                        .edges
                        .iter()
                        .map(|e| FieldValue::owned_any(e.clone()))
                        .collect();
                    Ok(Some(FieldValue::list(list)))
                })
            },
        ))
        .field(Field::new(
            "nodes",
            TypeRef::named_nn_list_nn(type_name),
            |ctx| {
                FieldFuture::new(async move {
                    let payload = ctx.parent_value.try_downcast_ref::<ConnectionPayload>()?;
                    let list: Vec<FieldValue> = payload
                        .edges
                        .iter()
                        .map(|e| FieldValue::owned_any(e.node.clone()))
                        .collect();
                    Ok(Some(FieldValue::list(list)))
                })
            },
        ));

    (connection, edge)
}
