use async_graphql_axum::*;
use axum::{
    Router,
    extract::State,
    response::{Html, IntoResponse},
    routing::get,
};
use turbograph::{Config, PoolConfig, TransactionConfig, TurboGraph};

#[tokio::main]
async fn main() {
    let server = TurboGraph::new(Config {
        pool: PoolConfig::ConnectionString(
            "postgres://postgres:Aa123456@localhost:5432/app-db".into(),
        ),
        schemas: vec!["public".into()],
        watch_pg: true,
    })
    .await
    .expect("failed to build schema");

    let app = Router::new()
        .route("/graphql", get(graphiql).post(graphql_handler))
        .with_state(server);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:4000").await.unwrap();
    println!("GraphQL playground: http://localhost:4000/graphql");
    axum::serve(listener, app).await.unwrap();
}

async fn graphql_handler(State(server): State<TurboGraph>, req: GraphQLRequest) -> GraphQLResponse {
    let tx_config = TransactionConfig {
        isolation_level: None,
        read_only: false,
        deferrable: false,
        timeout_seconds: None,
        role: Some("app_user".into()),
        settings: vec![("app.current_user_id".into(), "1".into())],
    };
    server
        .execute(req.into_inner().data(tx_config))
        .await
        .into()
}

async fn graphiql() -> impl IntoResponse {
    Html(TurboGraph::graphiql("/graphql"))
}
