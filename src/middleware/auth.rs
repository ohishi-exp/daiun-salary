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

#[derive(Clone)]
pub struct GatewaySecret(pub String);

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

/// Accepts either JWT auth (admin users) or gateway secret + tenant ID (external API clients).
pub async fn require_jwt_or_gateway(
    Extension(jwt_secret): Extension<JwtSecret>,
    Extension(gateway_secret): Extension<GatewaySecret>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // Try gateway auth first: X-Gateway-Secret + X-Tenant-Id
    let gw_secret = req
        .headers()
        .get("X-Gateway-Secret")
        .and_then(|v| v.to_str().ok());
    let gw_tenant = req
        .headers()
        .get("X-Tenant-Id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| Uuid::parse_str(v).ok());

    if let (Some(secret), Some(tenant_id)) = (gw_secret, gw_tenant) {
        if secret == gateway_secret.0 {
            let auth_user = AuthUser {
                user_id: Uuid::nil(),
                email: "api-client".to_string(),
                name: "API Client".to_string(),
                tenant_id,
                role: "api".to_string(),
            };
            req.extensions_mut().insert(TenantId(tenant_id));
            req.extensions_mut().insert(auth_user);
            return Ok(next.run(req).await);
        }
    }

    // Fallback to JWT auth
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
