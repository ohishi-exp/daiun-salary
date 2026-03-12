use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Extension, Json, Router,
};
use serde::Serialize;

use crate::middleware::auth::AuthUser;
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route(
        "/operations/{unko_no}/csv/{csv_type}",
        get(get_csv_as_json),
    )
}

#[derive(Debug, Serialize)]
pub struct CsvJsonResponse {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

async fn get_csv_as_json(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Path((unko_no, csv_type)): Path<(String, String)>,
) -> Result<Json<CsvJsonResponse>, StatusCode> {
    let filename = match csv_type.to_lowercase().as_str() {
        "kudguri" => "KUDGURI.csv",
        "kudgivt" => "KUDGIVT.csv",
        "kudgfry" | "ferry" => "KUDGFRY.csv",
        "kudgsir" => "KUDGSIR.csv",
        "speed" | "sokudo" => "SOKUDODATA.csv",
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    let key = format!("{}/unko/{}/{}", auth_user.tenant_id, unko_no, filename);

    let bytes = state
        .storage
        .download(&key)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;

    let text = String::from_utf8_lossy(&bytes);
    let mut lines = text.lines();

    let headers: Vec<String> = lines
        .next()
        .unwrap_or("")
        .split(',')
        .map(|h| h.trim().to_string())
        .collect();

    let rows: Vec<Vec<String>> = lines
        .filter(|l| !l.trim().is_empty())
        .map(|line| line.split(',').map(|f| f.trim().to_string()).collect())
        .collect();

    Ok(Json(CsvJsonResponse { headers, rows }))
}
