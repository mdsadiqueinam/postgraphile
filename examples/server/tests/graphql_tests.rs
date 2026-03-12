//! Integration tests for the generated GraphQL schema.
//!
//! Tests are driven by the seed data defined in `db/init.sql`:
//!   - 5 users  (alice, bob, charlie, diana, eve)
//!   - 8 posts  (7 published, 1 draft)
//!   - 8 comments
//!   - 6 tags   (rust, postgresql, graphql, api-design, performance, beginner)
//!   - 10 post_tag associations
//!
//! Requires a running PostgreSQL instance.  Start one with:
//!   docker compose up -d
//!
//! Override the connection string via:
//!   DATABASE_URL=postgres://... cargo test --test graphql_tests

use async_graphql::Request;
use serde_json::{Value as Json, json};
use turbograph::{Config, PoolConfig, TurboGraph};

// ── helpers ───────────────────────────────────────────────────────────────────

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:Aa123456@localhost:5432/app-db".into())
}

async fn make_schema() -> async_graphql::dynamic::Schema {
    let server = TurboGraph::new(Config {
        pool: PoolConfig::ConnectionString(db_url()),
        schemas: vec!["public".into()],
        watch_pg: false,
    })
    .await
    .expect("failed to build schema");
    server.schema().await
}

/// Execute a GraphQL query and panic on any errors, returning the `data` object
/// as a `serde_json::Value` for easy assertions.
async fn gql(schema: &async_graphql::dynamic::Schema, query: &str) -> Json {
    let resp = schema.execute(Request::new(query)).await;
    assert!(
        resp.errors.is_empty(),
        "GraphQL errors in query:\n{query}\nErrors: {errs:#?}",
        errs = resp.errors,
    );
    serde_json::to_value(resp.data).expect("failed to serialise response")
}

// ── schema introspection ──────────────────────────────────────────────────────

/// Every table in init.sql must appear as an `all{Table}` root field.
#[tokio::test]
async fn test_schema_exposes_all_tables() {
    let schema = make_schema().await;
    let data = gql(&schema, r#"{ __schema { queryType { fields { name } } } }"#).await;

    let fields: Vec<&str> = data["__schema"]["queryType"]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();

    for expected in &[
        "allUsers",
        "allPosts",
        "allComments",
        "allTags",
        "allPostTags",
    ] {
        assert!(fields.contains(expected), "missing query field: {expected}");
    }
}

// ── users ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_all_users_total_count() {
    let schema = make_schema().await;
    let data = gql(&schema, r#"{ allUsers { totalCount } }"#).await;
    assert_eq!(data["allUsers"]["totalCount"], json!(5));
}

#[tokio::test]
async fn test_user_fields_alice() {
    let schema = make_schema().await;
    let data = gql(
        &schema,
        r#"{
            allUsers(condition: { username: { equal: "alice" } }) {
                totalCount
                nodes { id username email bio is_active }
            }
        }"#,
    )
    .await;

    assert_eq!(data["allUsers"]["totalCount"], json!(1));
    let node = &data["allUsers"]["nodes"][0];
    assert_eq!(node["username"], json!("alice"));
    assert_eq!(node["email"], json!("alice@example.com"));
    assert_eq!(
        node["bio"],
        json!("Full-stack developer and coffee enthusiast.")
    );
    assert_eq!(node["is_active"], json!(true));
}

#[tokio::test]
async fn test_inactive_user_is_eve() {
    let schema = make_schema().await;
    let data = gql(
        &schema,
        r#"{
            allUsers(condition: { is_active: { equal: false } }) {
                totalCount
                nodes { username }
            }
        }"#,
    )
    .await;

    assert_eq!(data["allUsers"]["totalCount"], json!(1));
    assert_eq!(data["allUsers"]["nodes"][0]["username"], json!("eve"));
}

#[tokio::test]
async fn test_user_null_bio() {
    let schema = make_schema().await;
    let data = gql(
        &schema,
        r#"{
            allUsers(condition: { username: { equal: "diana" } }) {
                nodes { username bio }
            }
        }"#,
    )
    .await;

    // diana has no bio (NULL in seed data)
    assert!(
        data["allUsers"]["nodes"][0]["bio"].is_null(),
        "diana's bio should be null"
    );
}

#[tokio::test]
async fn test_users_order_by_username_asc() {
    let schema = make_schema().await;
    let data = gql(
        &schema,
        r#"{ allUsers(orderBy: [USERNAME_ASC]) { nodes { username } } }"#,
    )
    .await;

    let names: Vec<&str> = data["allUsers"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["username"].as_str().unwrap())
        .collect();

    assert_eq!(names, ["alice", "bob", "charlie", "diana", "eve"]);
}

#[tokio::test]
async fn test_users_pagination_first_page() {
    let schema = make_schema().await;
    let data = gql(
        &schema,
        r#"{
            allUsers(first: 2, offset: 0, orderBy: [ID_ASC]) {
                totalCount
                pageInfo { hasNextPage hasPreviousPage }
                edges { cursor node { username } }
            }
        }"#,
    )
    .await;

    let conn = &data["allUsers"];
    assert_eq!(conn["totalCount"], json!(5));
    assert_eq!(conn["pageInfo"]["hasNextPage"], json!(true));
    assert_eq!(conn["pageInfo"]["hasPreviousPage"], json!(false));
    assert_eq!(conn["edges"].as_array().unwrap().len(), 2);
    assert_eq!(conn["edges"][0]["node"]["username"], json!("alice"));
    assert_eq!(conn["edges"][1]["node"]["username"], json!("bob"));
}

#[tokio::test]
async fn test_users_pagination_second_page() {
    let schema = make_schema().await;
    let data = gql(
        &schema,
        r#"{
            allUsers(first: 2, offset: 2, orderBy: [ID_ASC]) {
                pageInfo { hasNextPage hasPreviousPage }
                edges { node { username } }
            }
        }"#,
    )
    .await;

    let conn = &data["allUsers"];
    // offset > 0 means there is a previous page
    assert_eq!(conn["pageInfo"]["hasPreviousPage"], json!(true));
    // offset(2) + edge_count(2) = 4 < total(5) → still more pages
    assert_eq!(conn["pageInfo"]["hasNextPage"], json!(true));
    assert_eq!(conn["edges"].as_array().unwrap().len(), 2);
    assert_eq!(conn["edges"][0]["node"]["username"], json!("charlie"));
    assert_eq!(conn["edges"][1]["node"]["username"], json!("diana"));
}

#[tokio::test]
async fn test_users_pagination_last_page() {
    let schema = make_schema().await;
    let data = gql(
        &schema,
        r#"{
            allUsers(first: 2, offset: 4, orderBy: [ID_ASC]) {
                pageInfo { hasNextPage hasPreviousPage }
                nodes { username }
            }
        }"#,
    )
    .await;

    let conn = &data["allUsers"];
    assert_eq!(conn["pageInfo"]["hasPreviousPage"], json!(true));
    assert_eq!(conn["pageInfo"]["hasNextPage"], json!(false));
    assert_eq!(conn["nodes"].as_array().unwrap().len(), 1);
    assert_eq!(conn["nodes"][0]["username"], json!("eve"));
}

// ── posts ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_all_posts_total_count() {
    let schema = make_schema().await;
    let data = gql(&schema, r#"{ allPosts { totalCount } }"#).await;
    assert_eq!(data["allPosts"]["totalCount"], json!(8));
}

#[tokio::test]
async fn test_published_posts_count() {
    let schema = make_schema().await;
    let data = gql(
        &schema,
        r#"{ allPosts(condition: { is_published: { equal: true } }) { totalCount } }"#,
    )
    .await;
    assert_eq!(data["allPosts"]["totalCount"], json!(7));
}

#[tokio::test]
async fn test_draft_post_title() {
    let schema = make_schema().await;
    let data = gql(
        &schema,
        r#"{
            allPosts(condition: { is_published: { equal: false } }) {
                totalCount
                nodes { title is_published }
            }
        }"#,
    )
    .await;

    assert_eq!(data["allPosts"]["totalCount"], json!(1));
    assert_eq!(
        data["allPosts"]["nodes"][0]["title"],
        json!("Draft: Async Rust Deep Dive")
    );
    assert_eq!(data["allPosts"]["nodes"][0]["is_published"], json!(false));
}

#[tokio::test]
async fn test_posts_by_author_alice() {
    let schema = make_schema().await;
    // alice is user id=1; she authored posts 1, 2, and 8
    let data = gql(
        &schema,
        r#"{
            allPosts(condition: { author_id: { equal: 1 } }, orderBy: [ID_ASC]) {
                totalCount
                nodes { title }
            }
        }"#,
    )
    .await;

    assert_eq!(data["allPosts"]["totalCount"], json!(3));
    let titles: Vec<&str> = data["allPosts"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["title"].as_str().unwrap())
        .collect();
    assert_eq!(titles[0], "Getting Started with Rust");
    assert_eq!(titles[1], "Understanding Ownership");
    assert_eq!(titles[2], "Turbograph from Scratch");
}

#[tokio::test]
async fn test_most_viewed_post() {
    let schema = make_schema().await;
    let data = gql(
        &schema,
        r#"{
            allPosts(orderBy: [VIEWS_DESC], first: 1) {
                nodes { title views }
            }
        }"#,
    )
    .await;

    let node = &data["allPosts"]["nodes"][0];
    assert_eq!(node["title"], json!("PostgreSQL Performance Tips"));
    assert_eq!(node["views"], json!(540));
}

#[tokio::test]
async fn test_posts_views_greater_than_300() {
    let schema = make_schema().await;
    // posts with views > 300:
    //   Getting Started with Rust (320), PostgreSQL Performance Tips (540), Turbograph from Scratch (430)
    let data = gql(
        &schema,
        r#"{ allPosts(condition: { views: { greaterThan: 300 } }) { totalCount } }"#,
    )
    .await;
    assert_eq!(data["allPosts"]["totalCount"], json!(3));
}

#[tokio::test]
async fn test_posts_views_in_list() {
    let schema = make_schema().await;
    // views == 320 or views == 430 → 2 posts
    let data = gql(
        &schema,
        r#"{ allPosts(condition: { views: { in: [320, 430] } }) { totalCount } }"#,
    )
    .await;
    assert_eq!(data["allPosts"]["totalCount"], json!(2));
}

// ── comments ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_all_comments_total_count() {
    let schema = make_schema().await;
    let data = gql(&schema, r#"{ allComments { totalCount } }"#).await;
    assert_eq!(data["allComments"]["totalCount"], json!(8));
}

#[tokio::test]
async fn test_comments_on_post_1() {
    let schema = make_schema().await;
    // "Getting Started with Rust" has 2 comments (from bob and charlie)
    let data = gql(
        &schema,
        r#"{
            allComments(condition: { post_id: { equal: 1 } }) {
                totalCount
                nodes { author_id body }
            }
        }"#,
    )
    .await;

    assert_eq!(data["allComments"]["totalCount"], json!(2));
    let author_ids: Vec<i64> = data["allComments"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["author_id"].as_i64().unwrap())
        .collect();
    // bob=2, charlie=3
    assert!(author_ids.contains(&2), "expected bob's comment");
    assert!(author_ids.contains(&3), "expected charlie's comment");
}

#[tokio::test]
async fn test_comments_by_bob() {
    let schema = make_schema().await;
    // bob (id=2) left comments on posts 1, 5, 8 → 3 comments
    let data = gql(
        &schema,
        r#"{ allComments(condition: { author_id: { equal: 2 } }) { totalCount } }"#,
    )
    .await;
    assert_eq!(data["allComments"]["totalCount"], json!(3));
}

#[tokio::test]
async fn test_comments_on_post_8() {
    let schema = make_schema().await;
    // "Turbograph from Scratch" has 2 comments (bob and charlie)
    let data = gql(
        &schema,
        r#"{ allComments(condition: { post_id: { equal: 8 } }) { totalCount } }"#,
    )
    .await;
    assert_eq!(data["allComments"]["totalCount"], json!(2));
}

// ── tags ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_all_tags_total_count() {
    let schema = make_schema().await;
    let data = gql(&schema, r#"{ allTags { totalCount } }"#).await;
    assert_eq!(data["allTags"]["totalCount"], json!(6));
}

#[tokio::test]
async fn test_tags_alphabetical_order() {
    let schema = make_schema().await;
    let data = gql(
        &schema,
        r#"{ allTags(orderBy: [NAME_ASC]) { nodes { name } } }"#,
    )
    .await;

    let names: Vec<&str> = data["allTags"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["name"].as_str().unwrap())
        .collect();

    assert_eq!(
        names,
        [
            "api-design",
            "beginner",
            "graphql",
            "performance",
            "postgresql",
            "rust"
        ]
    );
}

#[tokio::test]
async fn test_tag_filter_by_name() {
    let schema = make_schema().await;
    let data = gql(
        &schema,
        r#"{
            allTags(condition: { name: { equal: "graphql" } }) {
                totalCount
                nodes { id name }
            }
        }"#,
    )
    .await;

    assert_eq!(data["allTags"]["totalCount"], json!(1));
    assert_eq!(data["allTags"]["nodes"][0]["name"], json!("graphql"));
}

#[tokio::test]
async fn test_tags_in_list() {
    let schema = make_schema().await;
    let data = gql(
        &schema,
        r#"{ allTags(condition: { name: { in: ["rust", "graphql"] } }) { totalCount } }"#,
    )
    .await;
    assert_eq!(data["allTags"]["totalCount"], json!(2));
}

// ── post_tags ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_all_post_tags_total_count() {
    let schema = make_schema().await;
    let data = gql(&schema, r#"{ allPostTags { totalCount } }"#).await;
    // seed data inserts 11 (post_id, tag_id) pairs
    assert_eq!(data["allPostTags"]["totalCount"], json!(11));
}

#[tokio::test]
async fn test_post_tags_for_post_1() {
    let schema = make_schema().await;
    // post 1 ("Getting Started with Rust") is tagged with rust(1) and beginner(6)
    let data = gql(
        &schema,
        r#"{
            allPostTags(condition: { post_id: { equal: 1 } }, orderBy: [TAG_ID_ASC]) {
                totalCount
                nodes { post_id tag_id }
            }
        }"#,
    )
    .await;

    assert_eq!(data["allPostTags"]["totalCount"], json!(2));
    assert_eq!(data["allPostTags"]["nodes"][0]["tag_id"], json!(1)); // rust
    assert_eq!(data["allPostTags"]["nodes"][1]["tag_id"], json!(6)); // beginner
}

#[tokio::test]
async fn test_post_tags_for_tag_rust() {
    let schema = make_schema().await;
    // rust(1) is on posts 1, 2, 4 → 3 associations
    let data = gql(
        &schema,
        r#"{ allPostTags(condition: { tag_id: { equal: 1 } }) { totalCount } }"#,
    )
    .await;
    assert_eq!(data["allPostTags"]["totalCount"], json!(3));
}

#[tokio::test]
async fn test_post_tags_for_tag_graphql() {
    let schema = make_schema().await;
    // graphql(3) is on posts 5 and 8 → 2 associations
    let data = gql(
        &schema,
        r#"{ allPostTags(condition: { tag_id: { equal: 3 } }) { totalCount } }"#,
    )
    .await;
    assert_eq!(data["allPostTags"]["totalCount"], json!(2));
}

// ── mutation introspection ────────────────────────────────────────────────────

/// Verify mutation fields exist for tables (not materialized views or @omit'd).
#[tokio::test]
async fn test_schema_exposes_mutation_fields() {
    let schema = make_schema().await;
    let data = gql(
        &schema,
        r#"{ __schema { mutationType { fields { name } } } }"#,
    )
    .await;

    let fields: Vec<&str> = data["__schema"]["mutationType"]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();

    // Tables with mutations
    for expected in &[
        "createUser",
        "updateUser",
        "deleteUser",
        "createPost",
        "updatePost",
        "deletePost",
        "createComment",
        "createTag",
    ] {
        assert!(
            fields.contains(expected),
            "missing mutation field: {expected}"
        );
    }

    // post_tags has @omit create,update,delete → no mutations
    assert!(
        !fields.iter().any(|f| f.contains("PostTag")),
        "post_tags should have no mutations due to @omit"
    );
}

// ── create mutation ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_tag() {
    let schema = make_schema().await;
    let data = gql(
        &schema,
        r#"mutation { createTag(input: { name: "testing" }) { id name } }"#,
    )
    .await;

    let tag = &data["createTag"];
    assert_eq!(tag["name"], json!("testing"));
    assert!(tag["id"].as_i64().unwrap() > 0);

    // Clean up
    gql(
        &schema,
        r#"mutation { deleteTag(condition: { name: { equal: "testing" } }) { id } }"#,
    )
    .await;
}

// ── update mutation ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_update_user_bio() {
    let schema = make_schema().await;

    // Update diana's bio (was NULL)
    let data = gql(
        &schema,
        r#"mutation {
            updateUser(
                patch: { bio: "New bio for Diana" }
                condition: { username: { equal: "diana" } }
            ) { username bio }
        }"#,
    )
    .await;

    let users = data["updateUser"].as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0]["username"], json!("diana"));
    assert_eq!(users[0]["bio"], json!("New bio for Diana"));

    // Revert
    gql(
        &schema,
        r#"mutation {
            updateUser(
                patch: { bio: null }
                condition: { username: { equal: "diana" } }
            ) { id }
        }"#,
    )
    .await;
}

// ── delete mutation ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_and_delete_tag() {
    let schema = make_schema().await;

    // Create
    gql(
        &schema,
        r#"mutation { createTag(input: { name: "ephemeral" }) { id } }"#,
    )
    .await;

    // Delete
    let data = gql(
        &schema,
        r#"mutation { deleteTag(condition: { name: { equal: "ephemeral" } }) { name } }"#,
    )
    .await;

    let deleted = data["deleteTag"].as_array().unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0]["name"], json!("ephemeral"));
}
