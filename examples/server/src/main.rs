use async_graphql::http::GraphiQLSource;
use async_graphql_axum::*;
use axum::{
    Router,
    response::{Html, IntoResponse},
    routing::get,
};
use turbograph::{Config, PoolConfig, build_schema};

#[tokio::main]
async fn main() {
    let schema = build_schema(Config {
        pool: PoolConfig::ConnectionString(
            "postgres://postgres:Aa123456@localhost:5432/app-db".into(),
        ),
        schemas: vec!["public".into()],
    })
    .await
    .expect("failed to build schema");

    let app = Router::new().route("/graphql", get(graphiql).post_service(GraphQL::new(schema)));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:4000").await.unwrap();
    println!("GraphQL playground: http://localhost:4000/graphql");
    axum::serve(listener, app).await.unwrap();
}

async fn graphiql() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}
