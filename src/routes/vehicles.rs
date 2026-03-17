use axum::{extract::State, http::StatusCode, routing::get, Extension, Json, Router};

use crate::db::models::Vehicle;
use crate::middleware::auth::AuthUser;
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/vehicles", get(list_vehicles))
}

async fn list_vehicles(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<Vec<Vehicle>>, StatusCode> {
    let vehicles = sqlx::query_as::<_, Vehicle>(
        "SELECT * FROM vehicles WHERE tenant_id = $1 ORDER BY vehicle_cd",
    )
    .bind(auth_user.tenant_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(vehicles))
}
