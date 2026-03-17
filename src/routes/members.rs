use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, patch, post},
    Extension, Json, Router,
};

use crate::db::models::{InviteMemberRequest, TenantMemberListItem, UpdateMemberRoleRequest};
use crate::middleware::auth::AuthUser;
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/members", get(list_members))
        .route("/members", post(invite_member))
        .route("/members/{email}", patch(update_member_role))
        .route("/members/{email}", delete(remove_member))
}

async fn list_members(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<Vec<TenantMemberListItem>>, StatusCode> {
    let members = sqlx::query_as::<_, TenantMemberListItem>(
        "SELECT email, role, created_at FROM tenant_members WHERE tenant_id = $1 ORDER BY created_at",
    )
    .bind(auth_user.tenant_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(members))
}

async fn invite_member(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<InviteMemberRequest>,
) -> Result<Json<TenantMemberListItem>, (StatusCode, String)> {
    // admin のみ
    if auth_user.role != "admin" {
        return Err((StatusCode::FORBIDDEN, "admin only".to_string()));
    }

    // role バリデーション
    if req.role != "admin" && req.role != "member" {
        return Err((
            StatusCode::BAD_REQUEST,
            "role must be 'admin' or 'member'".to_string(),
        ));
    }

    let member = sqlx::query_as::<_, TenantMemberListItem>(
        r#"INSERT INTO tenant_members (tenant_id, email, role)
           VALUES ($1, $2, $3)
           RETURNING email, role, created_at"#,
    )
    .bind(auth_user.tenant_id)
    .bind(&req.email)
    .bind(&req.role)
    .fetch_one(&state.pool)
    .await
    .map_err(|e| {
        if e.to_string().contains("duplicate key") || e.to_string().contains("unique constraint") {
            (StatusCode::CONFLICT, "member already exists".to_string())
        } else {
            tracing::error!("Failed to invite member: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to invite member".to_string(),
            )
        }
    })?;

    Ok(Json(member))
}

async fn update_member_role(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Path(email): Path<String>,
    Json(req): Json<UpdateMemberRoleRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    if auth_user.role != "admin" {
        return Err((StatusCode::FORBIDDEN, "admin only".to_string()));
    }

    // 自分自身のロール変更は禁止
    if email == auth_user.email {
        return Err((
            StatusCode::BAD_REQUEST,
            "cannot change your own role".to_string(),
        ));
    }

    if req.role != "admin" && req.role != "member" {
        return Err((
            StatusCode::BAD_REQUEST,
            "role must be 'admin' or 'member'".to_string(),
        ));
    }

    let result =
        sqlx::query("UPDATE tenant_members SET role = $1 WHERE tenant_id = $2 AND email = $3")
            .bind(&req.role)
            .bind(auth_user.tenant_id)
            .bind(&email)
            .execute(&state.pool)
            .await
            .map_err(|e| {
                tracing::error!("Failed to update member role: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to update role".to_string(),
                )
            })?;

    if result.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, "member not found".to_string()));
    }

    Ok(StatusCode::NO_CONTENT)
}

async fn remove_member(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Path(email): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    if auth_user.role != "admin" {
        return Err((StatusCode::FORBIDDEN, "admin only".to_string()));
    }

    // 自分自身の削除は禁止
    if email == auth_user.email {
        return Err((
            StatusCode::BAD_REQUEST,
            "cannot remove yourself".to_string(),
        ));
    }

    // 最後の admin は削除不可
    let admin_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM tenant_members WHERE tenant_id = $1 AND role = 'admin'",
    )
    .bind(auth_user.tenant_id)
    .fetch_one(&state.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to count admins: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal error".to_string(),
        )
    })?;

    // 削除対象が admin かチェック
    let target_role: Option<String> =
        sqlx::query_scalar("SELECT role FROM tenant_members WHERE tenant_id = $1 AND email = $2")
            .bind(auth_user.tenant_id)
            .bind(&email)
            .fetch_optional(&state.pool)
            .await
            .map_err(|e| {
                tracing::error!("Failed to get member role: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_string(),
                )
            })?;

    match target_role {
        None => return Err((StatusCode::NOT_FOUND, "member not found".to_string())),
        Some(ref role) if role == "admin" && admin_count <= 1 => {
            return Err((
                StatusCode::BAD_REQUEST,
                "cannot remove the last admin".to_string(),
            ));
        }
        _ => {}
    }

    sqlx::query("DELETE FROM tenant_members WHERE tenant_id = $1 AND email = $2")
        .bind(auth_user.tenant_id)
        .bind(&email)
        .execute(&state.pool)
        .await
        .map_err(|e| {
            tracing::error!("Failed to remove member: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to remove member".to_string(),
            )
        })?;

    Ok(StatusCode::NO_CONTENT)
}
