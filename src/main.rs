mod auth;
mod csv_parser;
mod db;
mod middleware;
mod routes;
mod storage;

use std::sync::Arc;

use axum::{Extension, Router};
use sqlx::postgres::PgPoolOptions;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use crate::auth::google::GoogleTokenVerifier;
use crate::auth::jwt::JwtSecret;
use crate::middleware::auth::GatewaySecret;
use crate::routes::scraper::ScraperUrl;
use crate::storage::StorageBackend;

#[derive(Clone)]
pub struct AppState {
    pub pool: sqlx::PgPool,
    pub storage: Arc<dyn StorageBackend>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    dotenvy::dotenv().ok();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse()
        .expect("PORT must be a number");

    let google_client_id =
        std::env::var("GOOGLE_CLIENT_ID").expect("GOOGLE_CLIENT_ID must be set");
    let google_client_secret =
        std::env::var("GOOGLE_CLIENT_SECRET").expect("GOOGLE_CLIENT_SECRET must be set");
    let jwt_secret = std::env::var("JWT_SECRET").expect("JWT_SECRET must be set");
    let gateway_secret =
        std::env::var("GATEWAY_SECRET").unwrap_or_else(|_| "dev-gateway-secret".into());
    let scraper_url = std::env::var("SCRAPER_URL")
        .unwrap_or_else(|_| "http://localhost:8081".into());

    let google_verifier = GoogleTokenVerifier::new(google_client_id, google_client_secret);
    let jwt_secret = JwtSecret(jwt_secret);
    let gateway_secret = GatewaySecret(gateway_secret);

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    // R2 storage
    let bucket = std::env::var("R2_BUCKET").expect("R2_BUCKET must be set");
    let account_id = std::env::var("R2_ACCOUNT_ID").expect("R2_ACCOUNT_ID must be set");
    let access_key = std::env::var("R2_ACCESS_KEY").expect("R2_ACCESS_KEY must be set");
    let secret_key = std::env::var("R2_SECRET_KEY").expect("R2_SECRET_KEY must be set");
    let public_url = std::env::var("R2_PUBLIC_URL_BASE").ok();

    tracing::info!("Storage backend: R2 (bucket={})", bucket);
    let storage: Arc<dyn StorageBackend> = Arc::new(
        storage::R2Backend::new(bucket, account_id, access_key, secret_key, public_url)
            .expect("Failed to initialize R2 backend"),
    );

    let state = AppState { pool, storage };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .nest("/api", routes::router())
        .merge(routes::upload::internal_router())
        .layer(Extension(google_verifier))
        .layer(Extension(jwt_secret))
        .layer(Extension(ScraperUrl(scraper_url)))
        .layer(Extension(gateway_secret))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!("listening on 0.0.0.0:{port}");
    axum::serve(listener, app).await?;

    Ok(())
}
