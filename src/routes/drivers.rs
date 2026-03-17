use axum::{extract::State, http::StatusCode, routing::get, Extension, Json, Router};

use crate::db::models::Driver;
use crate::middleware::auth::AuthUser;
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/drivers", get(list_drivers))
}

async fn list_drivers(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<Vec<Driver>>, StatusCode> {
    let drivers = sqlx::query_as::<_, Driver>(
        "SELECT * FROM drivers WHERE tenant_id = $1 ORDER BY driver_cd",
    )
    .bind(auth_user.tenant_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(drivers))
}
