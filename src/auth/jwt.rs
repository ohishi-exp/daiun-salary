use uuid::Uuid;

use crate::db::models::User;

// JWT の claims / 検証は rust-alc-api の leaf crate `alc-auth-jwt` が SoT
// (Refs ippoan/rust-alc-api#410)。手動コピーをやめて re-export することで、
// alc 側の検証仕様変更 (env claim 等) に自動追従する。
// 既存の `crate::auth::jwt::...` import は無変更で通る。
pub use alc_auth_jwt::{
    create_refresh_token, current_env_label, hash_refresh_token, refresh_token_expires_at,
    verify_access_token, AccessTokenInput, AppClaims, JwtSecret, ACCESS_TOKEN_EXPIRY_SECS,
    REFRESH_TOKEN_EXPIRY_DAYS,
};

fn input_from_user(user: &User) -> AccessTokenInput {
    AccessTokenInput {
        sub: user.id,
        email: user.email.clone(),
        name: user.name.clone(),
        tenant_id: user.tenant_id,
        role: user.role.clone(),
    }
}

/// `&User` を受ける互換 wrapper。org_slug は本 repo では未使用 (None)。
pub fn create_access_token(
    user: &User,
    secret: &JwtSecret,
) -> Result<String, jsonwebtoken::errors::Error> {
    alc_auth_jwt::create_access_token(&input_from_user(user), secret, None)
}

/// テナント切替用: tenant_id / role を上書きして発行する互換 wrapper。
pub fn create_access_token_for_tenant(
    user: &User,
    tenant_id: Uuid,
    role: &str,
    secret: &JwtSecret,
) -> Result<String, jsonwebtoken::errors::Error> {
    let mut input = input_from_user(user);
    input.tenant_id = tenant_id;
    input.role = role.to_string();
    alc_auth_jwt::create_access_token(&input, secret, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

    /// STAGING_MODE を触る/読むテストの直列化 (set_var はプロセス全体に効くため)
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
        let _g = ENV_LOCK.lock().unwrap();
        let user = test_user();
        let secret = JwtSecret("test-secret-key-256-bits-long!!!".to_string());

        let token = create_access_token(&user, &secret).unwrap();
        let claims = verify_access_token(&token, &secret).unwrap();

        assert_eq!(claims.sub, user.id);
        assert_eq!(claims.email, user.email);
        assert_eq!(claims.tenant_id, user.tenant_id);
        // alc-auth-jwt は発行時に env claim を載せる (rust-alc-api #218)
        assert_eq!(claims.env.as_deref(), Some("prod"));
    }

    #[test]
    fn test_create_access_token_for_tenant_overrides() {
        let _g = ENV_LOCK.lock().unwrap();
        let user = test_user();
        let secret = JwtSecret("test-secret-key-256-bits-long!!!".to_string());
        let other_tenant = Uuid::new_v4();

        let token = create_access_token_for_tenant(&user, other_tenant, "viewer", &secret).unwrap();
        let claims = verify_access_token(&token, &secret).unwrap();

        assert_eq!(claims.sub, user.id);
        assert_eq!(claims.tenant_id, other_tenant);
        assert_eq!(claims.role, "viewer");
    }

    #[test]
    fn test_refresh_token_generation() {
        let (raw, hash) = create_refresh_token();
        assert!(raw.starts_with("rt_"));
        assert_eq!(hash, hash_refresh_token(&raw));
    }

    // alc-auth-jwt への contract test — 依存先の env claim 検証仕様 (rust-alc-api #218)
    // が将来の更新で変わったら本 repo の CI / ローカル test で気付けるよう固定する。

    fn make_token(env: Option<&str>, secret: &str) -> String {
        let now = Utc::now().timestamp();
        let claims = AppClaims {
            sub: Uuid::new_v4(),
            email: "t@example.com".to_string(),
            name: "t".to_string(),
            tenant_id: Uuid::new_v4(),
            role: "admin".to_string(),
            org_slug: None,
            env: env.map(|s| s.to_string()),
            iat: now,
            exp: now + 3600,
        };
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap()
    }

    #[test]
    fn verify_accepts_legacy_token_without_env() {
        let _g = ENV_LOCK.lock().unwrap();
        let secret = JwtSecret("test-secret".to_string());
        let token = make_token(None, &secret.0);
        assert!(verify_access_token(&token, &secret).is_ok());
    }

    #[test]
    fn verify_rejects_cross_env_token() {
        let _g = ENV_LOCK.lock().unwrap();
        let secret = JwtSecret("test-secret".to_string());
        let token = make_token(Some("staging"), &secret.0);
        let err = verify_access_token(&token, &secret).unwrap_err();
        assert!(matches!(
            err.kind(),
            jsonwebtoken::errors::ErrorKind::InvalidIssuer
        ));
    }

    #[test]
    fn current_env_label_staging_mode() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("STAGING_MODE", "true");
        assert_eq!(current_env_label(), "staging");
        std::env::remove_var("STAGING_MODE");
        assert_eq!(current_env_label(), "prod");
    }
}
