use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::Response,
    Extension,
};
use uuid::Uuid;

use crate::auth::jwt::{verify_access_token, JwtSecret};

#[derive(Debug, Clone)]
pub struct AuthUser {
    pub user_id: Uuid,
    pub email: String,
    pub name: String,
    pub tenant_id: Uuid,
    pub role: String,
}

#[derive(Debug, Clone, Copy)]
pub struct TenantId(pub Uuid);

pub async fn require_jwt(
    Extension(jwt_secret): Extension<JwtSecret>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let token = extract_bearer_token(&req).ok_or(StatusCode::UNAUTHORIZED)?;

    let claims = verify_access_token(token, &jwt_secret).map_err(|e| {
        tracing::warn!("JWT verification failed: {e}");
        StatusCode::UNAUTHORIZED
    })?;

    let auth_user = AuthUser {
        user_id: claims.sub,
        email: claims.email,
        name: claims.name,
        tenant_id: claims.tenant_id,
        role: claims.role,
    };

    req.extensions_mut().insert(TenantId(claims.tenant_id));
    req.extensions_mut().insert(auth_user);
    Ok(next.run(req).await)
}

fn extract_bearer_token(req: &Request) -> Option<&str> {
    req.headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}
