use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, put},
    Extension, Json, Router,
};
use uuid::Uuid;

use crate::db::models::{EventClassification, UpdateClassificationRequest};
use crate::middleware::auth::AuthUser;
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/event-classifications", get(list_event_classifications))
        .route("/event-classifications/{id}", put(update_classification))
}

async fn list_event_classifications(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<Vec<EventClassification>>, StatusCode> {
    let rows = sqlx::query_as::<_, EventClassification>(
        "SELECT * FROM event_classifications WHERE tenant_id = $1 ORDER BY event_cd",
    )
    .bind(auth_user.tenant_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(rows))
}

async fn update_classification(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateClassificationRequest>,
) -> Result<Json<EventClassification>, (StatusCode, String)> {
    let valid = ["work", "rest_split", "break", "ignore"];
    if !valid.contains(&body.classification.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "Invalid classification '{}'. Must be one of: {}",
                body.classification,
                valid.join(", ")
            ),
        ));
    }

    let row = sqlx::query_as::<_, EventClassification>(
        "UPDATE event_classifications SET classification = $1 WHERE id = $2 AND tenant_id = $3 RETURNING *",
    )
    .bind(&body.classification)
    .bind(id)
    .bind(auth_user.tenant_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    match row {
        Some(r) => Ok(Json(r)),
        None => Err((StatusCode::NOT_FOUND, "Not found".to_string())),
    }
}
