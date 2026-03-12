use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Extension, Json, Router,
};
use chrono::NaiveDate;
use serde::Serialize;
use uuid::Uuid;

use crate::db::models::{DailyHoursFilter, DailyWorkHours, DailyWorkSegment};
use crate::middleware::auth::AuthUser;
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/daily-hours", get(list_daily_hours))
        .route(
            "/daily-hours/{driver_id}/{date}/segments",
            get(get_daily_segments),
        )
}

#[derive(Debug, Serialize)]
pub struct DailyHoursResponse {
    pub items: Vec<DailyWorkHours>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

async fn list_daily_hours(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Query(filter): Query<DailyHoursFilter>,
) -> Result<Json<DailyHoursResponse>, StatusCode> {
    let tenant_id = auth_user.tenant_id;
    let page = filter.page.unwrap_or(1).max(1);
    let per_page = filter.per_page.unwrap_or(50).min(200);
    let offset = (page - 1) * per_page;

    let total: (i64,) = sqlx::query_as(
        r#"SELECT COUNT(*)::BIGINT FROM daily_work_hours
           WHERE tenant_id = $1
             AND ($2::UUID IS NULL OR driver_id = $2)
             AND ($3::DATE IS NULL OR work_date >= $3)
             AND ($4::DATE IS NULL OR work_date <= $4)"#,
    )
    .bind(tenant_id)
    .bind(filter.driver_id)
    .bind(filter.date_from)
    .bind(filter.date_to)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let items = sqlx::query_as::<_, DailyWorkHours>(
        r#"SELECT * FROM daily_work_hours
           WHERE tenant_id = $1
             AND ($2::UUID IS NULL OR driver_id = $2)
             AND ($3::DATE IS NULL OR work_date >= $3)
             AND ($4::DATE IS NULL OR work_date <= $4)
           ORDER BY work_date DESC, driver_id
           LIMIT $5 OFFSET $6"#,
    )
    .bind(tenant_id)
    .bind(filter.driver_id)
    .bind(filter.date_from)
    .bind(filter.date_to)
    .bind(per_page)
    .bind(offset)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(DailyHoursResponse {
        items,
        total: total.0,
        page,
        per_page,
    }))
}

#[derive(Debug, Serialize)]
pub struct SegmentsResponse {
    pub segments: Vec<DailyWorkSegment>,
}

async fn get_daily_segments(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Path((driver_id, date)): Path<(Uuid, NaiveDate)>,
) -> Result<Json<SegmentsResponse>, StatusCode> {
    let tenant_id = auth_user.tenant_id;

    let segments = sqlx::query_as::<_, DailyWorkSegment>(
        r#"SELECT * FROM daily_work_segments
           WHERE tenant_id = $1 AND driver_id = $2 AND work_date = $3
           ORDER BY start_at"#,
    )
    .bind(tenant_id)
    .bind(driver_id)
    .bind(date)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(SegmentsResponse { segments }))
}
