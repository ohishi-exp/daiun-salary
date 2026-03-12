use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
    Extension, Json, Router,
};
use chrono::{Duration, Utc};
use uuid::Uuid;

use crate::auth::jwt::hash_refresh_token;
use crate::db::models::{ApiTokenListItem, CreateApiTokenRequest, CreateApiTokenResponse};
use crate::middleware::auth::AuthUser;
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api-tokens", post(create_token))
        .route("/api-tokens", get(list_tokens))
        .route("/api-tokens/{id}", delete(revoke_token))
}

async fn create_token(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<CreateApiTokenRequest>,
) -> Result<Json<CreateApiTokenResponse>, (StatusCode, String)> {
    let tenant_id = auth_user.tenant_id;
    let created_by = auth_user.user_id;

    // Generate opaque token: daiun_ + 2 UUIDs (64 hex chars)
    let raw_token = format!(
        "daiun_{}{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    );
    let token_hash = hash_refresh_token(&raw_token);
    let token_prefix = raw_token[..14].to_string(); // "daiun_" + 8 hex chars

    let expires_at = req
        .expires_in_days
        .map(|days| Utc::now() + Duration::days(days));

    let id: Uuid = sqlx::query_scalar(
        r#"INSERT INTO api_tokens (tenant_id, created_by, name, token_hash, token_prefix, expires_at)
           VALUES ($1, $2, $3, $4, $5, $6)
           RETURNING id"#,
    )
    .bind(tenant_id)
    .bind(created_by)
    .bind(&req.name)
    .bind(&token_hash)
    .bind(&token_prefix)
    .bind(expires_at)
    .fetch_one(&state.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to create API token: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create token".to_string(),
        )
    })?;

    Ok(Json(CreateApiTokenResponse {
        id,
        name: req.name,
        token: raw_token,
        token_prefix,
    }))
}

async fn list_tokens(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<Vec<ApiTokenListItem>>, StatusCode> {
    let items = sqlx::query_as::<_, ApiTokenListItem>(
        r#"SELECT id, name, token_prefix, expires_at, revoked_at, last_used_at, created_at
           FROM api_tokens
           WHERE tenant_id = $1
           ORDER BY created_at DESC"#,
    )
    .bind(auth_user.tenant_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(items))
}

async fn revoke_token(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let result = sqlx::query(
        r#"UPDATE api_tokens SET revoked_at = now()
           WHERE id = $1 AND tenant_id = $2 AND revoked_at IS NULL"#,
    )
    .bind(id)
    .bind(auth_user.tenant_id)
    .execute(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if result.rows_affected() == 0 {
        return Err(StatusCode::NOT_FOUND);
    }

    Ok(StatusCode::NO_CONTENT)
}
