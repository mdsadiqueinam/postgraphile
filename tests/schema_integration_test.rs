use turbograph::{Config, PoolConfig, build_schema};

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:Aa123456@localhost:5432/app-db".to_string())
}

/// Replicates `all{to_pascal_case(table_name)}` for standard snake_case postgres table names.
fn table_to_query_field(table_name: &str) -> String {
    let pascal: String = table_name
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().to_string() + chars.as_str(),
            }
        })
        .collect();
    format!("all{}", pascal)
}

/// Verifies that every table in the `public` schema has a corresponding
/// root Query field (e.g. `users` → `allUsers`) in the generated schema.
#[tokio::test]
async fn all_db_tables_are_present_in_schema() {
    let url = db_url();

    // Build the schema from the live database.
    let server = build_schema(Config {
        pool: PoolConfig::ConnectionString(url.clone()),
        schemas: vec!["public".to_string()],
        watch_pg: false,
    })
    .await
    .expect("build_schema failed");

    // Query the actual table names directly from Postgres.
    let (client, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("failed to connect to database for table introspection");
    tokio::spawn(async move { conn.await.ok() });

    let rows = client
        .query(
            "SELECT c.relname \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public' AND c.relkind IN ('r', 'm') \
             ORDER BY c.relname",
            &[],
        )
        .await
        .expect("pg_catalog query failed");

    let table_names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(!table_names.is_empty(), "no tables found in the database");

    // Execute GraphQL introspection against the built schema.
    let result = server
        .execute("{ __schema { queryType { fields { name } } } }".into())
        .await;

    assert!(
        result.errors.is_empty(),
        "GraphQL introspection returned errors: {:?}",
        result.errors
    );

    let data = result.data.into_json().unwrap();
    let schema_fields: Vec<String> = data["__schema"]["queryType"]["fields"]
        .as_array()
        .expect("queryType.fields should be an array")
        .iter()
        .map(|f| f["name"].as_str().unwrap().to_string())
        .collect();

    // Assert every DB table is represented by an `all*` query field.
    for table_name in &table_names {
        let expected_field = table_to_query_field(table_name);
        assert!(
            schema_fields.contains(&expected_field),
            "table '{}': expected query field '{}' not found in schema.\nAvailable fields: {:?}",
            table_name,
            expected_field,
            schema_fields,
        );
    }
}
