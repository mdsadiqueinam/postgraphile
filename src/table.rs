use serde::{Deserialize, Serialize};
use std::sync::LazyLock;
use tokio_postgres::types::Type;

/// Omit is used to determine which operations (create, read, update, delete) should be omitted for a given table or column based on its comment.
/// The comment can contain an @omit annotation followed by a comma-separated list of operations to omit. For example:
/// - `@omit read,update` would indicate that the read and update operations should be omitted for that table or column.
/// - `@omit` without any operations would indicate that all operations
/// from this struct false means it is not omitted, true means it is omitted
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Omit {
    create: bool,
    read: bool,
    update: bool,
    delete: bool,
}

impl Omit {
    pub(crate) fn new(comment: &str) -> Self {
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
pub enum Relkind {
    Table,
    MaterializedView,
}

#[derive(Clone, Debug)]
pub struct Column {
    id: u32,
    table_oid: u32,
    name: String,
    comment: String,
    r#type: Option<Type>,
    nullable: bool,
    omit: Omit,
}

impl Column {
    pub(crate) fn form_row(row: &tokio_postgres::Row) -> Self {
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

    pub fn table_oid(&self) -> &u32 {
        &self.table_oid
    }

    pub fn name(&self) -> &String {
        &self.name
    }
}

#[derive(Clone, Debug)]
pub struct Table {
    oid: u32,
    name: String,
    schema_name: String,
    relkind: Relkind,
    comment: String,
    columns: Vec<Column>,
    omit: Omit,
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

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn oid(&self) -> &u32 {
        &self.oid
    }
}
