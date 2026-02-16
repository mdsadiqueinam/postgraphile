use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::LazyLock};
use tokio_postgres::types::Type;

/// Omit is used to determine which operations (create, read, update, delete) should be omitted for a given table or column based on its comment.
/// The comment can contain an @omit annotation followed by a comma-separated list of operations to omit. For example:
/// - `@omit read,update` would indicate that the read and update operations should be omitted for that table or column.
/// - `@omit` without any operations would indicate that all operations
/// from this struct false means it is not omitted, true means it is omitted
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct Omit {
    pub create: bool,
    pub read: bool,
    pub update: bool,
    pub delete: bool,
}

impl Omit {
    pub fn new(comment: &str) -> Self {
        static OMIT_REGEX: LazyLock<regex::Regex> =
            LazyLock::new(|| regex::Regex::new(r"@omit\s+([^\s]+)").unwrap());

        let have_omit = comment.contains("@omit");

        // omit all if there is only omit string
        let mut omit = Omit {
            read: have_omit,
            create: have_omit,
            update: have_omit,
            delete: have_omit,
        };

        if let Some(caps) = OMIT_REGEX.captures(comment) {
            let res = &caps[1];
            let parts = res.split(",").collect::<Vec<&str>>();

            omit.read = parts.contains(&"read");
            omit.create = parts.contains(&"create");
            omit.update = parts.contains(&"update");
            omit.delete = parts.contains(&"delete");
        }

        return omit;
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Relkind {
    Table,
    MaterializedView,
}

#[derive(Clone, Debug)]
pub(crate) struct Column {
    pub(crate) id: u32,
    pub table_oid: u32,
    pub name: String,
    pub comment: String,
    pub r#type: Option<Type>,
    pub nullable: bool,
    pub omit: Omit,
}

impl Column {
    pub fn form_row(row: &tokio_postgres::Row) -> Self {
        let table_oid = row.try_get::<_, u32>(0).unwrap();
        let column_id = row.try_get::<_, u32>(1).unwrap();
        let column_name = row.try_get::<_, String>(2).unwrap();
        let type_oid = row.try_get::<_, u32>(3).unwrap();
        let nullable = row.try_get::<_, bool>(4).unwrap();
        let comment = row.try_get::<_, String>(5).unwrap_or("".to_string());
        let omit = Omit::new(&comment);

        return Self {
            id: column_id,
            table_oid,
            name: column_name,
            comment,
            r#type: Type::from_oid(type_oid),
            nullable,
            omit,
        };
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Table {
    pub(crate) oid: u32,
    pub(crate) name: String,
    pub(crate) schema_name: String,
    pub(crate) relkind: Relkind,
    pub(crate) comment: String,
    pub(crate) columns: Vec<Column>,
    pub(crate) omit: Omit,
}

impl Table {
    pub(crate) fn from_row(row: &tokio_postgres::Row) -> Self {
        let oid = row.try_get::<_, u32>(0).unwrap();
        let schema_name = row.try_get::<_, String>(0).unwrap();
        let table_name = row.try_get::<_, String>(1).unwrap();
        let relkind_str = row.try_get::<_, String>(2).unwrap();
        let comment = row.try_get::<_, String>(3).unwrap_or("".to_string());
        let omit = Omit::new(&comment);

        return Self {
            oid,
            schema_name,
            name: table_name,
            relkind: if relkind_str == "r" {
                Relkind::Table
            } else {
                Relkind::MaterializedView
            },
            comment,
            columns: Vec::new(),
            omit,
        };
    }

    pub(crate) fn push_column(&mut self, column: Column) {
        self.columns.push(column);
    }
}

fn map_columns_to_table(tables: Vec<Table>, columns: Vec<Column>) -> Vec<Table> {
    let mut table_map: HashMap<u32, Table> = tables
        .into_iter()
        .map(|table| (table.oid.clone(), table))
        .collect();

    for col in columns.into_iter() {
        if let Some(table) = table_map.get_mut(&col.table_oid) {
            table.push_column(col);
        }
    }

    return table_map.into_values().collect();
}

pub(crate) async fn get_tables(
    pool: &deadpool_postgres::Pool,
    schemas: &Vec<String>,
) -> Vec<Table> {
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

    let table_oids = tables.iter().map(|t| &t.oid).collect::<Vec<&u32>>();

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
