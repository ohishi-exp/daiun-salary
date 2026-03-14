use std::convert::Infallible;

use axum::{
    extract::{Query, State},
    response::sse::{Event, KeepAlive, Sse},
    Extension, Json, Router,
    routing::{get, post},
};
use chrono::{DateTime, NaiveDate, Utc};
use futures::stream::Stream;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::middleware::auth::TenantId;
use crate::AppState;

#[derive(Clone)]
pub struct ScraperUrl(pub String);

#[derive(Deserialize)]
pub struct ScrapeRequest {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub comp_id: Option<String>,
    #[serde(default)]
    pub skip_upload: bool,
}

#[derive(Serialize, Deserialize)]
pub struct ScrapeResult {
    pub comp_id: String,
    pub status: String,
    pub message: String,
}

#[derive(Serialize, Deserialize)]
pub struct ScrapeResponse {
    pub results: Vec<ScrapeResult>,
}

/// SSE イベントのパース用
#[derive(Deserialize)]
struct SseEvent {
    event: Option<String>,
    comp_id: Option<String>,
    status: Option<String>,
    message: Option<String>,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct ScrapeHistoryItem {
    pub id: Uuid,
    pub target_date: NaiveDate,
    pub comp_id: String,
    pub status: String,
    pub message: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Deserialize)]
pub struct HistoryQuery {
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 {
    50
}

/// Cloud Run メタデータサーバーから ID トークンを取得
async fn get_id_token(client: &Client, audience: &str) -> Result<String, String> {
    let url = format!(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience={}",
        audience
    );
    let res = client
        .get(&url)
        .header("Metadata-Flavor", "Google")
        .send()
        .await
        .map_err(|e| format!("Metadata server error: {e}"))?;

    if !res.status().is_success() {
        return Err(format!("Metadata server returned {}", res.status()));
    }

    res.text().await.map_err(|e| format!("Failed to read ID token: {e}"))
}

/// SSE ストリームプロキシ: dtako-scraper の SSE レスポンスを中継 + DB 保存
async fn trigger_scrape(
    State(state): State<AppState>,
    Extension(scraper_url): Extension<ScraperUrl>,
    Extension(tenant_id): Extension<TenantId>,
    Json(req): Json<ScrapeRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (axum::http::StatusCode, String)> {
    let client = Client::new();

    let mut request = client
        .post(format!("{}/scrape", scraper_url.0))
        .json(&serde_json::json!({
            "start_date": req.start_date,
            "end_date": req.end_date,
            "comp_id": req.comp_id,
            "skip_upload": req.skip_upload,
        }))
        .timeout(std::time::Duration::from_secs(600));

    // Cloud Run 上ではメタデータサーバーから ID トークンを取得
    if let Ok(token) = get_id_token(&client, &scraper_url.0).await {
        request = request.bearer_auth(token);
    }

    let res = request.send().await.map_err(|e| {
        (
            axum::http::StatusCode::BAD_GATEWAY,
            format!("Scraper connection error: {e}"),
        )
    })?;

    if !res.status().is_success() {
        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        return Err((
            axum::http::StatusCode::BAD_GATEWAY,
            format!("Scraper returned {status}: {body}"),
        ));
    }

    // target_date の決定: start_date を使用（省略時は前日）
    let target_date_str = req.start_date.unwrap_or_else(|| {
        (chrono::Local::now() - chrono::Duration::days(1))
            .format("%Y-%m-%d")
            .to_string()
    });
    let target_date = NaiveDate::parse_from_str(&target_date_str, "%Y-%m-%d")
        .unwrap_or_else(|_| chrono::Local::now().date_naive());
    let tid = tenant_id.0;
    let pool = state.pool.clone();

    // SSE ストリームを中継
    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(32);

    tokio::spawn(async move {
        let mut stream = res.bytes_stream();
        use futures::StreamExt;
        let mut buffer = String::new();

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    buffer.push_str(&String::from_utf8_lossy(&bytes));

                    // SSE の "data: ...\n\n" を1行ずつパース
                    while let Some(pos) = buffer.find("\n\n") {
                        let message = buffer[..pos].to_string();
                        buffer = buffer[pos + 2..].to_string();

                        for line in message.lines() {
                            if let Some(data) = line.strip_prefix("data:") {
                                let data = data.trim();
                                if !data.is_empty() {
                                    // DB に result イベントを保存
                                    if let Ok(evt) = serde_json::from_str::<SseEvent>(data) {
                                        if evt.event.as_deref() == Some("result") {
                                            if let Some(ref comp_id) = evt.comp_id {
                                                let status = evt.status.as_deref().unwrap_or("error");
                                                let message = evt.message.as_deref();
                                                let _ = sqlx::query(
                                                    r#"INSERT INTO scrape_history (tenant_id, target_date, comp_id, status, message)
                                                       VALUES ($1, $2, $3, $4, $5)"#,
                                                )
                                                .bind(tid)
                                                .bind(target_date)
                                                .bind(comp_id)
                                                .bind(status)
                                                .bind(message)
                                                .execute(&pool)
                                                .await;
                                            }
                                        }
                                    }

                                    let _ = tx.send(Ok(Event::default().data(data.to_string()))).await;
                                }
                            }
                        }
                    }
                }
                Err(_) => break,
            }
        }
    });

    let stream = ReceiverStream::new(rx);
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// スクレイプ履歴を取得
async fn get_scrape_history(
    State(state): State<AppState>,
    Extension(tenant_id): Extension<TenantId>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<Vec<ScrapeHistoryItem>>, (axum::http::StatusCode, String)> {
    let rows = sqlx::query_as::<_, ScrapeHistoryItem>(
        r#"SELECT id, target_date, comp_id, status, message, created_at
           FROM scrape_history
           WHERE tenant_id = $1
           ORDER BY created_at DESC
           LIMIT $2 OFFSET $3"#,
    )
    .bind(tenant_id.0)
    .bind(query.limit)
    .bind(query.offset)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("DB error: {e}"),
        )
    })?;

    Ok(Json(rows))
}

pub fn router() -> Router<AppState> {
    Router::new().route("/scraper/trigger", post(trigger_scrape))
}

pub fn history_router() -> Router<AppState> {
    Router::new().route("/scraper/history", get(get_scrape_history))
}
