use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Extension, Json, Router,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::google::GoogleTokenVerifier;
use crate::auth::jwt::{
    self, create_access_token, create_access_token_for_tenant, create_refresh_token,
    hash_refresh_token, refresh_token_expires_at, JwtSecret,
};
use crate::db::models::{SwitchTenantRequest, Tenant, User, UserTenantInfo};
use crate::middleware::auth::AuthUser;
use crate::AppState;

pub fn public_router() -> Router<AppState> {
    Router::new()
        .route("/auth/google/code", post(google_code_login))
        .route("/auth/refresh", post(refresh_token))
}

pub fn protected_router() -> Router<AppState> {
    Router::new()
        .route("/auth/me", get(me))
        .route("/auth/logout", post(logout))
        .route("/auth/tenants", get(list_tenants))
        .route("/auth/switch-tenant", post(switch_tenant))
}

#[derive(Debug, Deserialize)]
pub struct GoogleCodeRequest {
    pub code: String,
    pub redirect_uri: String,
}

#[derive(Debug, Serialize)]
pub struct AuthResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: i64,
    pub user: UserResponse,
}

#[derive(Debug, Serialize)]
pub struct UserResponse {
    pub id: Uuid,
    pub email: String,
    pub name: String,
    pub tenant_id: Uuid,
    pub role: String,
}

#[derive(Debug, Serialize)]
pub struct MeResponse {
    pub id: Uuid,
    pub email: String,
    pub name: String,
    pub tenant_id: Uuid,
    pub role: String,
    pub tenants: Vec<UserTenantInfo>,
}

async fn google_code_login(
    State(state): State<AppState>,
    Extension(verifier): Extension<GoogleTokenVerifier>,
    Extension(jwt_secret): Extension<JwtSecret>,
    Json(body): Json<GoogleCodeRequest>,
) -> Result<Json<AuthResponse>, StatusCode> {
    let google_claims = verifier
        .exchange_code(&body.code, &body.redirect_uri)
        .await
        .map_err(|e| {
            tracing::warn!("Google code exchange failed: {e}");
            StatusCode::UNAUTHORIZED
        })?;

    // Find or create user
    let existing_user = sqlx::query_as::<_, User>("SELECT * FROM users WHERE google_sub = $1")
        .bind(&google_claims.sub)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let user = match existing_user {
        Some(user) => user,
        None => {
            // 招待済みメンバーかチェック（tenant_members に事前登録されているか）
            let invited = sqlx::query_as::<_, (uuid::Uuid, String)>(
                "SELECT tenant_id, role FROM tenant_members WHERE email = $1 LIMIT 1",
            )
            .bind(&google_claims.email)
            .fetch_optional(&state.pool)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let (tenant_id, role) = if let Some((tid, role)) = invited {
                // 招待済み: 既存テナントに参加
                (tid, role)
            } else {
                // 未招待: 新テナント作成
                let tenant_name = google_claims
                    .email
                    .split('@')
                    .nth(1)
                    .unwrap_or("default")
                    .to_string();

                let tenant = sqlx::query_as::<_, Tenant>(
                    "INSERT INTO tenants (name) VALUES ($1) RETURNING *",
                )
                .bind(&tenant_name)
                .fetch_one(&state.pool)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                // tenant_members にメンバー登録
                sqlx::query(
                    "INSERT INTO tenant_members (tenant_id, email, role) VALUES ($1, $2, 'admin') ON CONFLICT DO NOTHING",
                )
                .bind(tenant.id)
                .bind(&google_claims.email)
                .execute(&state.pool)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                (tenant.id, "admin".to_string())
            };

            sqlx::query_as::<_, User>(
                r#"
                INSERT INTO users (tenant_id, google_sub, email, name, role)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING *
                "#,
            )
            .bind(tenant_id)
            .bind(&google_claims.sub)
            .bind(&google_claims.email)
            .bind(&google_claims.name)
            .bind(&role)
            .fetch_one(&state.pool)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        }
    };

    let access_token =
        create_access_token(&user, &jwt_secret).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (raw_refresh, refresh_hash) = create_refresh_token();
    let expires_at = refresh_token_expires_at();

    sqlx::query(
        "UPDATE users SET refresh_token_hash = $1, refresh_token_expires_at = $2 WHERE id = $3",
    )
    .bind(&refresh_hash)
    .bind(expires_at)
    .bind(user.id)
    .execute(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(AuthResponse {
        access_token,
        refresh_token: raw_refresh,
        expires_in: jwt::ACCESS_TOKEN_EXPIRY_SECS,
        user: UserResponse {
            id: user.id,
            email: user.email,
            name: user.name,
            tenant_id: user.tenant_id,
            role: user.role,
        },
    }))
}

#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
}

#[derive(Debug, Serialize)]
pub struct RefreshResponse {
    pub access_token: String,
    pub expires_in: i64,
}

async fn refresh_token(
    State(state): State<AppState>,
    Extension(jwt_secret): Extension<JwtSecret>,
    Json(body): Json<RefreshRequest>,
) -> Result<Json<RefreshResponse>, StatusCode> {
    let token_hash = hash_refresh_token(&body.refresh_token);

    let user = sqlx::query_as::<_, User>(
        r#"
        SELECT * FROM users
        WHERE refresh_token_hash = $1
          AND refresh_token_expires_at > NOW()
        "#,
    )
    .bind(&token_hash)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .ok_or(StatusCode::UNAUTHORIZED)?;

    // user.tenant_id は最後に切り替えたテナント
    let access_token =
        create_access_token(&user, &jwt_secret).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(RefreshResponse {
        access_token,
        expires_in: jwt::ACCESS_TOKEN_EXPIRY_SECS,
    }))
}

async fn me(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<MeResponse>, StatusCode> {
    let tenants = sqlx::query_as::<_, UserTenantInfo>(
        "SELECT t.id AS tenant_id, t.name AS tenant_name FROM tenants t JOIN tenant_members tm ON tm.tenant_id = t.id WHERE tm.email = $1",
    )
    .bind(&auth_user.email)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(MeResponse {
        id: auth_user.user_id,
        email: auth_user.email,
        name: auth_user.name,
        tenant_id: auth_user.tenant_id,
        role: auth_user.role,
        tenants,
    }))
}

async fn list_tenants(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<Vec<UserTenantInfo>>, StatusCode> {
    let tenants = sqlx::query_as::<_, UserTenantInfo>(
        "SELECT t.id AS tenant_id, t.name AS tenant_name FROM tenants t JOIN tenant_members tm ON tm.tenant_id = t.id WHERE tm.email = $1",
    )
    .bind(&auth_user.email)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(tenants))
}

#[derive(Debug, Serialize)]
pub struct SwitchTenantResponse {
    pub access_token: String,
    pub expires_in: i64,
    pub tenant_id: Uuid,
    pub tenant_name: String,
}

async fn switch_tenant(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Extension(jwt_secret): Extension<JwtSecret>,
    Json(body): Json<SwitchTenantRequest>,
) -> Result<Json<SwitchTenantResponse>, StatusCode> {
    // tenant_members で所属確認 + role 取得
    #[derive(sqlx::FromRow)]
    struct TenantWithRole {
        name: String,
        member_role: String,
    }
    let tenant = sqlx::query_as::<_, TenantWithRole>(
        "SELECT t.name, tm.role AS member_role FROM tenants t JOIN tenant_members tm ON tm.tenant_id = t.id WHERE t.id = $1 AND tm.email = $2",
    )
    .bind(body.tenant_id)
    .bind(&auth_user.email)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .ok_or(StatusCode::FORBIDDEN)?;

    // users.tenant_id と role を更新
    sqlx::query("UPDATE users SET tenant_id = $1, role = $2 WHERE id = $3")
        .bind(body.tenant_id)
        .bind(&tenant.member_role)
        .bind(auth_user.user_id)
        .execute(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // ユーザーレコード取得（更新後）
    let user = sqlx::query_as::<_, User>("SELECT * FROM users WHERE id = $1")
        .bind(auth_user.user_id)
        .fetch_one(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let access_token =
        create_access_token_for_tenant(&user, body.tenant_id, &tenant.member_role, &jwt_secret)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(SwitchTenantResponse {
        access_token,
        expires_in: jwt::ACCESS_TOKEN_EXPIRY_SECS,
        tenant_id: body.tenant_id,
        tenant_name: tenant.name,
    }))
}

async fn logout(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<StatusCode, StatusCode> {
    sqlx::query(
        "UPDATE users SET refresh_token_hash = NULL, refresh_token_expires_at = NULL WHERE id = $1",
    )
    .bind(auth_user.user_id)
    .execute(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::NO_CONTENT)
}
