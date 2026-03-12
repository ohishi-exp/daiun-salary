use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
    Extension, Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::jwt::hash_refresh_token;
use crate::middleware::auth::GatewaySecret;
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/gateway/validate-token", post(validate_token))
}

#[derive(Debug, Deserialize)]
struct ValidateTokenRequest {
    token: String,
}

#[derive(Debug, Serialize)]
struct ValidateTokenResponse {
    tenant_id: Uuid,
    token_name: String,
}

async fn validate_token(
    State(state): State<AppState>,
    Extension(gateway_secret): Extension<GatewaySecret>,
    headers: HeaderMap,
    Json(req): Json<ValidateTokenRequest>,
) -> Result<Json<ValidateTokenResponse>, StatusCode> {
    let secret = headers
        .get("X-Gateway-Secret")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    if secret != gateway_secret.0 {
        return Err(StatusCode::UNAUTHORIZED);
    }

    // Hash the provided token and look up
    let token_hash = hash_refresh_token(&req.token);

    let row: Option<(Uuid, String)> = sqlx::query_as(
        r#"SELECT tenant_id, name FROM api_tokens
           WHERE token_hash = $1
             AND revoked_at IS NULL
             AND (expires_at IS NULL OR expires_at > $2)"#,
    )
    .bind(&token_hash)
    .bind(Utc::now())
    .fetch_optional(&state.pool)
    .await
    .map_err(|e| {
        tracing::error!("Token validation query failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let (tenant_id, token_name) = row.ok_or(StatusCode::UNAUTHORIZED)?;

    // Update last_used_at (fire-and-forget)
    let pool = state.pool.clone();
    let hash = token_hash.clone();
    tokio::spawn(async move {
        let _ = sqlx::query("UPDATE api_tokens SET last_used_at = now() WHERE token_hash = $1")
            .bind(&hash)
            .execute(&pool)
            .await;
    });

    Ok(Json(ValidateTokenResponse {
        tenant_id,
        token_name,
    }))
}
