use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;

const JWKS_CACHE_TTL_SECS: u64 = 3600;
const GOOGLE_JWKS_URL: &str = "https://www.googleapis.com/oauth2/v3/certs";
const GOOGLE_ISSUER: &str = "https://accounts.google.com";
const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

#[derive(Debug, Deserialize)]
pub struct GoogleClaims {
    pub sub: String,
    pub email: String,
    #[serde(default)]
    pub name: String,
    pub picture: Option<String>,
    #[serde(default)]
    pub email_verified: bool,
    pub aud: String,
    pub iss: String,
    pub exp: u64,
}

#[derive(Debug, Deserialize, Clone)]
struct JwkKey {
    kid: String,
    n: String,
    e: String,
    #[serde(default)]
    kty: String,
    #[serde(default)]
    alg: String,
}

#[derive(Debug, Deserialize)]
struct JwksResponse {
    keys: Vec<JwkKey>,
}

struct CachedJwks {
    keys: Vec<JwkKey>,
    fetched_at: std::time::Instant,
}

#[derive(Debug, Deserialize)]
struct GoogleTokenResponse {
    id_token: String,
}

#[derive(Clone)]
pub struct GoogleTokenVerifier {
    client_id: String,
    client_secret: String,
    http_client: Client,
    jwks_cache: Arc<RwLock<Option<CachedJwks>>>,
}

impl GoogleTokenVerifier {
    pub fn new(client_id: String, client_secret: String) -> Self {
        Self {
            client_id,
            client_secret,
            http_client: Client::new(),
            jwks_cache: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn exchange_code(
        &self,
        code: &str,
        redirect_uri: &str,
    ) -> Result<GoogleClaims, VerifyError> {
        let resp = self
            .http_client
            .post(GOOGLE_TOKEN_URL)
            .form(&[
                ("code", code),
                ("client_id", &self.client_id),
                ("client_secret", &self.client_secret),
                ("redirect_uri", redirect_uri),
                ("grant_type", "authorization_code"),
            ])
            .send()
            .await
            .map_err(|e| {
                tracing::warn!("Google token exchange request failed: {e}");
                VerifyError::TokenExchangeFailed
            })?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            tracing::warn!("Google token exchange failed: {body}");
            return Err(VerifyError::TokenExchangeFailed);
        }

        let token_resp: GoogleTokenResponse = resp.json().await.map_err(|e| {
            tracing::warn!("Failed to parse Google token response: {e}");
            VerifyError::TokenExchangeFailed
        })?;

        self.verify(&token_resp.id_token).await
    }

    pub async fn verify(&self, id_token: &str) -> Result<GoogleClaims, VerifyError> {
        let header = decode_header(id_token).map_err(|_| VerifyError::InvalidToken)?;
        let kid = header.kid.ok_or(VerifyError::InvalidToken)?;
        let key = self.get_key(&kid).await?;
        let decoding_key =
            DecodingKey::from_rsa_components(&key.n, &key.e).map_err(|_| VerifyError::InvalidKey)?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[GOOGLE_ISSUER, "accounts.google.com"]);
        validation.set_audience(&[&self.client_id]);

        let token_data =
            decode::<GoogleClaims>(id_token, &decoding_key, &validation).map_err(|e| {
                tracing::warn!("Google ID token verification failed: {e}");
                VerifyError::InvalidToken
            })?;

        if !token_data.claims.email_verified {
            return Err(VerifyError::EmailNotVerified);
        }

        Ok(token_data.claims)
    }

    async fn get_key(&self, kid: &str) -> Result<JwkKey, VerifyError> {
        {
            let cache = self.jwks_cache.read().await;
            if let Some(cached) = cache.as_ref() {
                if cached.fetched_at.elapsed().as_secs() < JWKS_CACHE_TTL_SECS {
                    if let Some(key) = cached.keys.iter().find(|k| k.kid == kid) {
                        return Ok(key.clone());
                    }
                }
            }
        }

        let resp = self
            .http_client
            .get(GOOGLE_JWKS_URL)
            .send()
            .await
            .map_err(|_| VerifyError::JwksFetchFailed)?;

        let jwks: JwksResponse = resp
            .json()
            .await
            .map_err(|_| VerifyError::JwksFetchFailed)?;

        let key = jwks
            .keys
            .iter()
            .find(|k| k.kid == kid)
            .cloned()
            .ok_or(VerifyError::KeyNotFound)?;

        {
            let mut cache = self.jwks_cache.write().await;
            *cache = Some(CachedJwks {
                keys: jwks.keys,
                fetched_at: std::time::Instant::now(),
            });
        }

        Ok(key)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    #[error("invalid token")]
    InvalidToken,
    #[error("invalid key")]
    InvalidKey,
    #[error("email not verified")]
    EmailNotVerified,
    #[error("failed to fetch JWKS")]
    JwksFetchFailed,
    #[error("key not found in JWKS")]
    KeyNotFound,
    #[error("failed to exchange authorization code")]
    TokenExchangeFailed,
}
