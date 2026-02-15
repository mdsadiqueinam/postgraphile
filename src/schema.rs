use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) enum Relkind {
    #[serde(rename = "r")]
    Table,
    #[serde(rename = "m")]
    MaterializedView,
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct Column {
    pub name: String,
    pub comment: String,
    pub data_type: String,
    pub nullable: bool,
    pub omit: Omit,
}

impl Column {
    pub fn form_row(row: &tokio_postgres::Row) -> Self {
        let column_name = row.try_get::<_, String>(0).unwrap();
        let comment = row.try_get::<_, String>(1).unwrap_or("".to_string());
        let data_type = row.try_get::<_, String>(2).unwrap();
        let nullable = row.try_get::<_, bool>(3).unwrap();
        let omit = Omit::new(&comment);

        return Self {
            name: column_name,
            comment,
            data_type,
            nullable,
            omit,
        };
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub(crate) struct Table {
    pub schema_name: String,
    pub object_name: String,
    pub relkind: Relkind,
    pub comment: String,
    pub columns: Vec<Column>,
    pub omit: Omit,
}

impl Table {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        let schema_name = row.try_get::<_, String>(0).unwrap();
        let object_name = row.try_get::<_, String>(1).unwrap();
        let relkind_str = row.try_get::<_, String>(2).unwrap();
        let comment = row.try_get::<_, String>(3).unwrap_or("".to_string());
        let omit = Omit::new(&comment);

        return Self {
            schema_name,
            object_name,
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
}

pub async fn get_tables(client: &deadpool_postgres::Object) {
    let schemas = vec!["public", "inventory", "sales"];
    let rows = client
        .query(
            "SELECT
                n.nspname AS schema_name,
                c.relname AS object_name,
                c.relkind,
                d.description AS comment
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
            WHERE c.relkind IN ('r', 'm')
            -- Filter by an array of schema names
            AND n.nspname = ANY($1)
            ORDER BY schema_name, type, object_name;",
            &[&schemas],
        )
        .await
        .unwrap();
}
