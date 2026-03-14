use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{delete, get},
    Extension, Json, Router,
};
use serde::Deserialize;

use crate::db::models::{Operation, OperationFilter, OperationListItem, OperationsResponse};
use crate::middleware::auth::AuthUser;
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/operations", get(list_operations))
        .route("/operations/calendar", get(calendar_dates))
        .route("/operations/{unko_no}", get(get_operation))
        .route("/operations/{unko_no}", delete(delete_operation))
}

#[derive(Deserialize)]
struct CalendarQuery {
    year: i32,
    month: i32,
}

#[derive(serde::Serialize)]
struct CalendarResponse {
    year: i32,
    month: u32,
    dates: Vec<CalendarDateEntry>,
}

#[derive(serde::Serialize)]
struct CalendarDateEntry {
    date: chrono::NaiveDate,
    count: i64,
}

async fn calendar_dates(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Query(q): Query<CalendarQuery>,
) -> Result<Json<CalendarResponse>, StatusCode> {
    let month = q.month as u32;
    let date_from = chrono::NaiveDate::from_ymd_opt(q.year, month, 1)
        .ok_or(StatusCode::BAD_REQUEST)?;
    let date_to = if month == 12 {
        chrono::NaiveDate::from_ymd_opt(q.year + 1, 1, 1)
    } else {
        chrono::NaiveDate::from_ymd_opt(q.year, month + 1, 1)
    }
    .ok_or(StatusCode::BAD_REQUEST)?
    .pred_opt()
    .ok_or(StatusCode::BAD_REQUEST)?;

    let rows = sqlx::query_as::<_, (chrono::NaiveDate, i64)>(
        r#"SELECT reading_date, COUNT(*)::BIGINT
           FROM operations
           WHERE tenant_id = $1
             AND reading_date >= $2
             AND reading_date <= $3
           GROUP BY reading_date
           ORDER BY reading_date"#,
    )
    .bind(auth_user.tenant_id)
    .bind(date_from)
    .bind(date_to)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let dates = rows
        .into_iter()
        .map(|(date, count)| CalendarDateEntry { date, count })
        .collect();

    Ok(Json(CalendarResponse {
        year: q.year,
        month: month,
        dates,
    }))
}

async fn list_operations(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Query(filter): Query<OperationFilter>,
) -> Result<Json<OperationsResponse>, StatusCode> {
    let tenant_id = auth_user.tenant_id;
    let page = filter.page.unwrap_or(1).max(1);
    let per_page = filter.per_page.unwrap_or(50).min(200);
    let offset = (page - 1) * per_page;

    let total: (i64,) = sqlx::query_as(
        r#"SELECT COUNT(*)::BIGINT FROM operations o
           LEFT JOIN drivers d ON o.driver_id = d.id
           LEFT JOIN vehicles v ON o.vehicle_id = v.id
           WHERE o.tenant_id = $1
             AND ($2::DATE IS NULL OR o.reading_date >= $2)
             AND ($3::DATE IS NULL OR o.reading_date <= $3)
             AND ($4::TEXT IS NULL OR d.driver_cd = $4)
             AND ($5::TEXT IS NULL OR v.vehicle_cd = $5)"#,
    )
    .bind(tenant_id)
    .bind(filter.date_from)
    .bind(filter.date_to)
    .bind(&filter.driver_cd)
    .bind(&filter.vehicle_cd)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let operations = sqlx::query_as::<_, OperationListItem>(
        r#"SELECT o.id, o.unko_no, o.crew_role, o.reading_date, o.operation_date,
                  d.driver_name, v.vehicle_name,
                  o.total_distance, o.safety_score, o.economy_score, o.total_score
           FROM operations o
           LEFT JOIN drivers d ON o.driver_id = d.id
           LEFT JOIN vehicles v ON o.vehicle_id = v.id
           WHERE o.tenant_id = $1
             AND ($2::DATE IS NULL OR o.reading_date >= $2)
             AND ($3::DATE IS NULL OR o.reading_date <= $3)
             AND ($4::TEXT IS NULL OR d.driver_cd = $4)
             AND ($5::TEXT IS NULL OR v.vehicle_cd = $5)
           ORDER BY o.reading_date DESC, o.unko_no
           LIMIT $6 OFFSET $7"#,
    )
    .bind(tenant_id)
    .bind(filter.date_from)
    .bind(filter.date_to)
    .bind(&filter.driver_cd)
    .bind(&filter.vehicle_cd)
    .bind(per_page)
    .bind(offset)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(OperationsResponse {
        operations,
        total: total.0,
        page,
        per_page,
    }))
}

async fn get_operation(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Path(unko_no): Path<String>,
) -> Result<Json<Vec<Operation>>, StatusCode> {
    let ops = sqlx::query_as::<_, Operation>(
        "SELECT * FROM operations WHERE tenant_id = $1 AND unko_no = $2 ORDER BY crew_role",
    )
    .bind(auth_user.tenant_id)
    .bind(&unko_no)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if ops.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(ops))
}

async fn delete_operation(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Path(unko_no): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let result =
        sqlx::query("DELETE FROM operations WHERE tenant_id = $1 AND unko_no = $2")
            .bind(auth_user.tenant_id)
            .bind(&unko_no)
            .execute(&state.pool)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if result.rows_affected() == 0 {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(StatusCode::NO_CONTENT)
}
