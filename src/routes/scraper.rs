use std::convert::Infallible;

use axum::{
    response::sse::{Event, KeepAlive, Sse},
    Extension, Json, Router,
    routing::post,
};
use futures::stream::Stream;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

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

/// SSE ストリームプロキシ: dtako-scraper の SSE レスポンスを中継
async fn trigger_scrape(
    Extension(scraper_url): Extension<ScraperUrl>,
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

pub fn router() -> Router<AppState> {
    Router::new().route("/scraper/trigger", post(trigger_scrape))
}
