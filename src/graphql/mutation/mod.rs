use std::collections::HashMap;
use std::sync::Arc;

use async_graphql::Value as GqlValue;
use async_graphql::dynamic::{Field, FieldFuture, InputObject, InputValue, TypeRef};
use deadpool_postgres::Pool;

use crate::models::table::{Column, Table};
use crate::models::transaction::TransactionConfig;

use super::type_mapping::condition_type_ref;

mod executor;

/// All types and fields generated for a table's mutations.
pub struct GeneratedMutation {
    /// Mutation root fields (createX, updateX, deleteX).
    pub fields: Vec<Field>,
    /// Input object types to register (CreateXInput, UpdateXPatch).
    pub input_objects: Vec<InputObject>,
}

/// Generates create / update / delete mutation fields for a single table.
///
/// Respects `@omit create`, `@omit update`, `@omit delete` annotations at
/// both the table and column level.  Materialized views are automatically
/// excluded (handled by `Table::omit_*` methods).
pub fn generate_mutation(table: Arc<Table>, pool: Arc<Pool>) -> GeneratedMutation {
    let mut fields = Vec::new();
    let mut input_objects = Vec::new();

    let type_name = table.type_name();
    let tbl_schema = table.schema_name().to_string();
    let tbl_name = table.name().to_string();

    // Column indices used for condition WHERE clauses (reuses {Type}Condition)
    let all_columns: Arc<Vec<Arc<Column>>> = Arc::new(table.columns().to_vec());
    let cond_col_map: Arc<HashMap<String, usize>> = Arc::new(
        all_columns
            .iter()
            .enumerate()
            .filter(|(_, c)| !c.omit_read() && condition_type_ref(c).is_some())
            .map(|(i, c)| (c.name().to_string(), i))
            .collect(),
    );

    // ── CREATE ────────────────────────────────────────────────────────────
    if !table.omit_create() {
        let input_name = format!("Create{}Input", type_name);
        let mut create_input = InputObject::new(&input_name);

        let mut create_col_map = HashMap::new();
        for (i, col) in all_columns.iter().enumerate() {
            if col.omit_create() {
                continue;
            }
            if let Some(tr) = condition_type_ref(col) {
                let type_ref = if !col.nullable() && !col.has_default() {
                    TypeRef::named_nn(tr.to_string())
                } else {
                    tr
                };
                create_input = create_input.field(InputValue::new(col.name().as_str(), type_ref));
                create_col_map.insert(col.name().to_string(), i);
            }
        }

        let create_col_map = Arc::new(create_col_map);
        let cols = all_columns.clone();
        let p = pool.clone();
        let s = tbl_schema.clone();
        let n = tbl_name.clone();
        let inp_ref = input_name.clone();

        let field = Field::new(
            format!("create{}", type_name),
            TypeRef::named(type_name.clone()),
            move |ctx| {
                let input_pairs: Vec<(String, GqlValue)> = ctx
                    .args
                    .get("input")
                    .and_then(|v| v.object().ok())
                    .map(|obj| {
                        obj.iter()
                            .map(|(k, v)| (k.to_string(), v.as_value().clone()))
                            .collect()
                    })
                    .unwrap_or_default();

                let pool = p.clone();
                let schema = s.clone();
                let name = n.clone();
                let columns = cols.clone();
                let col_map = create_col_map.clone();
                let tx_config = ctx.data_opt::<TransactionConfig>().cloned();

                FieldFuture::new(async move {
                    executor::execute_create(
                        &pool, &schema, &name, input_pairs, &columns, &col_map, tx_config,
                    )
                    .await
                })
            },
        )
        .argument(InputValue::new("input", TypeRef::named_nn(inp_ref)));

        fields.push(field);
        input_objects.push(create_input);
    }

    // ── UPDATE ────────────────────────────────────────────────────────────
    if !table.omit_update() {
        let patch_name = format!("Update{}Patch", type_name);
        let mut patch_input = InputObject::new(&patch_name);

        let mut update_col_map = HashMap::new();
        for (i, col) in all_columns.iter().enumerate() {
            if col.omit_update() {
                continue;
            }
            if let Some(tr) = condition_type_ref(col) {
                patch_input = patch_input.field(InputValue::new(col.name().as_str(), tr));
                update_col_map.insert(col.name().to_string(), i);
            }
        }

        let update_col_map = Arc::new(update_col_map);
        let cols = all_columns.clone();
        let cm = cond_col_map.clone();
        let p = pool.clone();
        let s = tbl_schema.clone();
        let n = tbl_name.clone();
        let patch_ref = patch_name.clone();
        let cond_ref = format!("{}Condition", type_name);

        let field = Field::new(
            format!("update{}", type_name),
            TypeRef::named_nn_list_nn(type_name.clone()),
            move |ctx| {
                let patch_pairs: Vec<(String, GqlValue)> = ctx
                    .args
                    .get("patch")
                    .and_then(|v| v.object().ok())
                    .map(|obj| {
                        obj.iter()
                            .map(|(k, v)| (k.to_string(), v.as_value().clone()))
                            .collect()
                    })
                    .unwrap_or_default();

                let condition_pairs: Option<Vec<(String, GqlValue)>> = ctx
                    .args
                    .get("condition")
                    .and_then(|v| v.object().ok())
                    .map(|obj| {
                        obj.iter()
                            .map(|(k, v)| (k.to_string(), v.as_value().clone()))
                            .collect()
                    });

                let pool = p.clone();
                let schema = s.clone();
                let name = n.clone();
                let columns = cols.clone();
                let ucm = update_col_map.clone();
                let ccm = cm.clone();
                let tx_config = ctx.data_opt::<TransactionConfig>().cloned();

                FieldFuture::new(async move {
                    executor::execute_update(
                        &pool,
                        &schema,
                        &name,
                        patch_pairs,
                        condition_pairs,
                        &columns,
                        &ucm,
                        &ccm,
                        tx_config,
                    )
                    .await
                })
            },
        )
        .argument(InputValue::new("patch", TypeRef::named_nn(patch_ref)))
        .argument(InputValue::new("condition", TypeRef::named(cond_ref)));

        fields.push(field);
        input_objects.push(patch_input);
    }

    // ── DELETE ─────────────────────────────────────────────────────────────
    if !table.omit_delete() {
        let cols = all_columns.clone();
        let cm = cond_col_map.clone();
        let p = pool.clone();
        let s = tbl_schema;
        let n = tbl_name;
        let cond_ref = format!("{}Condition", type_name);

        let field = Field::new(
            format!("delete{}", type_name),
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

                let pool = p.clone();
                let schema = s.clone();
                let name = n.clone();
                let columns = cols.clone();
                let ccm = cm.clone();
                let tx_config = ctx.data_opt::<TransactionConfig>().cloned();

                FieldFuture::new(async move {
                    executor::execute_delete(
                        &pool,
                        &schema,
                        &name,
                        condition_pairs,
                        &columns,
                        &ccm,
                        tx_config,
                    )
                    .await
                })
            },
        )
        .argument(InputValue::new("condition", TypeRef::named(cond_ref)));

        fields.push(field);
    }

    GeneratedMutation {
        fields,
        input_objects,
    }
}
