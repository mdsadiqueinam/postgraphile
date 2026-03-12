use std::sync::Arc;

use async_graphql::dynamic::{Object, Schema};
use deadpool_postgres::Pool;
use tokio::sync::RwLock;

use crate::graphql;
use crate::models::config::{Config, PoolConfig};

/// The main entry point for consuming the library.
///
/// `TurboGraph` wraps the dynamically-built GraphQL schema and, when
/// `watch_pg` is enabled, transparently handles live-reloading in the
/// background. It is cheaply cloneable (backed by `Arc`) and can be
/// used as shared state in any async web framework (axum, actix-web,
/// poem, etc.).
///
/// # Example (axum)
///
/// ```rust,ignore
/// let server = TurboGraph::new(config).await?;
/// let app = Router::new()
///     .route("/graphql", get(graphiql).post(handler))
///     .with_state(server);
/// ```
#[derive(Clone)]
pub struct TurboGraph {
    schema: Arc<RwLock<Schema>>,
}

impl TurboGraph {
    /// Build the GraphQL schema from the database described by `config`.
    ///
    /// When [`Config::watch_pg`] is `true`, event triggers are installed and a
    /// background task is spawned that automatically swaps in a freshly built
    /// schema whenever a DDL change is detected.
    pub async fn new(config: Config) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let watch_pg = config.watch_pg;

        let connection_url = if watch_pg {
            match &config.pool {
                PoolConfig::ConnectionString(url) => Some(url.clone()),
                PoolConfig::Pool(_) => {
                    return Err("watch_pg requires PoolConfig::ConnectionString".into());
                }
            }
        } else {
            None
        };

        let pool = Arc::new(crate::db::pool::resolve(config.pool)?);
        let built_schema = rebuild_schema(&pool, &config.schemas).await?;
        let schema = Arc::new(RwLock::new(built_schema));

        if watch_pg {
            let url = connection_url.unwrap();
            crate::db::watch::install_triggers(&pool).await?;
            crate::db::watch::start_watching(url, pool, config.schemas, schema.clone()).await?;
        }

        Ok(Self { schema })
    }

    /// Execute a GraphQL request against the current schema.
    pub async fn execute(&self, request: async_graphql::Request) -> async_graphql::Response {
        let schema = self.schema.read().await.clone();
        schema.execute(request).await
    }

    /// Returns the GraphiQL HTML page pointing at the given `endpoint`.
    pub fn graphiql(endpoint: &str) -> String {
        async_graphql::http::GraphiQLSource::build()
            .endpoint(endpoint)
            .finish()
    }

    /// Returns a clone of the current underlying dynamic schema.
    pub async fn schema(&self) -> Schema {
        self.schema.read().await.clone()
    }
}

/// Builds a schema from the current database state.
///
/// Used for the initial build and for automatic rebuilds triggered by DDL
/// changes.
pub(crate) async fn rebuild_schema(
    pool: &Arc<Pool>,
    schemas: &[String],
) -> Result<Schema, Box<dyn std::error::Error + Send + Sync>> {
    let tables = crate::db::introspect::get_tables(pool, schemas).await;

    let mut query_root = Object::new("Query");
    let mut builder = Schema::build("Query", None, None);

    builder = builder.register(graphql::make_page_info_type());

    for table in tables {
        if table.omit_read() {
            continue;
        }

        let table = Arc::new(table);
        let entity = graphql::generate_entity(table.clone());
        let gq = graphql::generate_query(table, pool.clone());

        query_root = query_root.field(gq.query_field);
        builder = builder
            .register(entity)
            .register(gq.condition_type)
            .register(gq.order_by_enum)
            .register(gq.connection_type)
            .register(gq.edge_type);

        for ft in gq.condition_filter_types {
            builder = builder.register(ft);
        }
    }

    let schema = builder.register(query_root).finish()?;
    Ok(schema)
}
