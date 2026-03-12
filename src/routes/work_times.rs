use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Extension, Json, Router,
};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use crate::middleware::auth::AuthUser;
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/work-times", get(list_work_times))
}

#[derive(Debug, Deserialize)]
pub struct WorkTimesFilter {
    pub driver_id: Option<Uuid>,
    pub date_from: Option<NaiveDate>,
    pub date_to: Option<NaiveDate>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct WorkTimeItem {
    pub id: Uuid,
    pub driver_id: Uuid,
    pub work_date: NaiveDate,
    pub unko_no: String,
    pub segment_index: i32,
    pub start_at: DateTime<Utc>,
    pub end_at: DateTime<Utc>,
    pub work_minutes: i32,
    pub labor_minutes: i32,
}

#[derive(Debug, Serialize)]
pub struct WorkTimesResponse {
    pub items: Vec<WorkTimeItem>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

async fn list_work_times(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Query(filter): Query<WorkTimesFilter>,
) -> Result<Json<WorkTimesResponse>, StatusCode> {
    let tenant_id = auth_user.tenant_id;
    let page = filter.page.unwrap_or(1).max(1);
    let per_page = filter.per_page.unwrap_or(50).min(200);
    let offset = (page - 1) * per_page;

    let total: (i64,) = sqlx::query_as(
        r#"SELECT COUNT(*)::BIGINT FROM daily_work_segments
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

    let items = sqlx::query_as::<_, WorkTimeItem>(
        r#"SELECT s.id, s.driver_id, s.work_date, s.unko_no, s.segment_index,
                  s.start_at, s.end_at, s.work_minutes, s.labor_minutes
           FROM daily_work_segments s
           JOIN drivers d ON d.id = s.driver_id
           WHERE s.tenant_id = $1
             AND ($2::UUID IS NULL OR s.driver_id = $2)
             AND ($3::DATE IS NULL OR s.work_date >= $3)
             AND ($4::DATE IS NULL OR s.work_date <= $4)
           ORDER BY s.work_date ASC, d.driver_cd, s.start_at
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

    Ok(Json(WorkTimesResponse {
        items,
        total: total.0,
        page,
        per_page,
    }))
}
