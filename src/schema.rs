use crate::table::{Column, Table};
use std::collections::HashMap;

fn map_columns_to_table(tables: Vec<Table>, columns: Vec<Column>) -> Vec<Table> {
    let mut table_map: HashMap<u32, Table> = tables
        .into_iter()
        .map(|table| (table.oid().clone(), table))
        .collect();

    for col in columns.into_iter() {
        if let Some(table) = table_map.get_mut(col.table_oid()) {
            table.push_column(col);
        }
    }

    return table_map.into_values().collect();
}

pub async fn get_tables(pool: &deadpool_postgres::Pool, schemas: &Vec<String>) -> Vec<Table> {
    let client = pool.get().await.unwrap();
    let tables: Vec<Table> = client
        .query(
            "SELECT 
                c.oid, 
                n.nspname AS schema_name,
                c.relname AS table_name,
                c.relkind,
                pg_catalog.obj_description(c.oid, 'pg_class') AS comment
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace     -- To filter schema
            WHERE n.nspname = ANY($1)
            AND c.relkind IN ('r', 'm')
            ORDER BY n.nspname, c.relname;",
            &[schemas],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| Table::from_row(r))
        .collect();

    let table_oids = tables.iter().map(|t| t.oid()).collect::<Vec<&u32>>();

    let columns = client
        .query(
            "SELECT 
                a.attrelid AS table_oid, 
                a.attnum AS column_id,
                a.attname AS column_name, 
                a.atttypid AS type_oid, 
                NOT a.attnotnull AS nullable,
                pg_catalog.col_description(a.attrelid, a.attnum) AS comment
            FROM 
                pg_catalog.pg_attribute a
            WHERE 
                a.attrelid = ANY($1)              -- Your Table OID
                AND a.attnum > 0 
                AND NOT a.attisdropped
            ORDER BY 
                a.attnum;",
            &[&table_oids],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| Column::form_row(r))
        .collect::<Vec<Column>>();

    return map_columns_to_table(tables, columns);
}
