use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::db::models::User;

pub const ACCESS_TOKEN_EXPIRY_SECS: i64 = 3600;
pub const REFRESH_TOKEN_EXPIRY_DAYS: i64 = 30;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppClaims {
    pub sub: Uuid,
    pub email: String,
    pub name: String,
    pub tenant_id: Uuid,
    pub role: String,
    pub iat: i64,
    pub exp: i64,
}

#[derive(Clone)]
pub struct JwtSecret(pub String);

pub fn create_access_token(
    user: &User,
    secret: &JwtSecret,
) -> Result<String, jsonwebtoken::errors::Error> {
    let now = Utc::now();
    let claims = AppClaims {
        sub: user.id,
        email: user.email.clone(),
        name: user.name.clone(),
        tenant_id: user.tenant_id,
        role: user.role.clone(),
        iat: now.timestamp(),
        exp: (now + Duration::seconds(ACCESS_TOKEN_EXPIRY_SECS)).timestamp(),
    };

    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.0.as_bytes()),
    )
}

pub fn create_access_token_for_tenant(
    user: &User,
    tenant_id: Uuid,
    role: &str,
    secret: &JwtSecret,
) -> Result<String, jsonwebtoken::errors::Error> {
    let now = Utc::now();
    let claims = AppClaims {
        sub: user.id,
        email: user.email.clone(),
        name: user.name.clone(),
        tenant_id,
        role: role.to_string(),
        iat: now.timestamp(),
        exp: (now + Duration::seconds(ACCESS_TOKEN_EXPIRY_SECS)).timestamp(),
    };

    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.0.as_bytes()),
    )
}

pub fn verify_access_token(
    token: &str,
    secret: &JwtSecret,
) -> Result<AppClaims, jsonwebtoken::errors::Error> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;

    let token_data = decode::<AppClaims>(
        token,
        &DecodingKey::from_secret(secret.0.as_bytes()),
        &validation,
    )?;

    Ok(token_data.claims)
}

pub fn create_refresh_token() -> (String, String) {
    let raw = format!("rt_{}", Uuid::new_v4().simple());
    let hash = hash_refresh_token(&raw);
    (raw, hash)
}

pub fn refresh_token_expires_at() -> chrono::DateTime<Utc> {
    Utc::now() + Duration::days(REFRESH_TOKEN_EXPIRY_DAYS)
}

pub fn hash_refresh_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_user() -> User {
        User {
            id: Uuid::new_v4(),
            tenant_id: Uuid::new_v4(),
            google_sub: "google-sub-123".to_string(),
            email: "test@example.com".to_string(),
            name: "Test User".to_string(),
            role: "admin".to_string(),
            refresh_token_hash: None,
            refresh_token_expires_at: None,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn test_create_and_verify_access_token() {
        let user = test_user();
        let secret = JwtSecret("test-secret-key-256-bits-long!!!".to_string());

        let token = create_access_token(&user, &secret).unwrap();
        let claims = verify_access_token(&token, &secret).unwrap();

        assert_eq!(claims.sub, user.id);
        assert_eq!(claims.email, user.email);
        assert_eq!(claims.tenant_id, user.tenant_id);
    }

    #[test]
    fn test_refresh_token_generation() {
        let (raw, hash) = create_refresh_token();
        assert!(raw.starts_with("rt_"));
        assert_eq!(hash, hash_refresh_token(&raw));
    }
}
