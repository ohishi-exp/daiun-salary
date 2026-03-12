use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Extension, Json, Router,
};
use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::middleware::auth::AuthUser;
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/restraint-report", get(get_restraint_report))
}

#[derive(Debug, Deserialize)]
pub struct RestraintReportFilter {
    pub driver_id: Uuid,
    pub year: i32,
    pub month: u32,
}

// --- Response DTOs ---

#[derive(Debug, Serialize)]
pub struct RestraintReportResponse {
    pub driver_id: Uuid,
    pub driver_name: String,
    pub year: i32,
    pub month: u32,
    pub max_restraint_minutes: i32,
    pub days: Vec<RestraintDayRow>,
    pub weekly_subtotals: Vec<WeeklySubtotal>,
    pub monthly_total: MonthlyTotal,
}

#[derive(Debug, Serialize)]
pub struct RestraintDayRow {
    pub date: NaiveDate,
    pub is_holiday: bool,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub operations: Vec<OperationDetail>,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
    pub break_minutes: i32,
    pub restraint_total_minutes: i32,
    pub restraint_cumulative_minutes: i32,
    pub drive_average_minutes: f64,
    pub rest_period_minutes: Option<i32>,
    pub remarks: String,
}

#[derive(Debug, Serialize)]
pub struct OperationDetail {
    pub unko_no: String,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
    pub break_minutes: i32,
    pub restraint_minutes: i32,
}

#[derive(Debug, Serialize)]
pub struct WeeklySubtotal {
    pub week_end_date: NaiveDate,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
    pub break_minutes: i32,
    pub restraint_minutes: i32,
}

#[derive(Debug, Serialize)]
pub struct MonthlyTotal {
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
    pub break_minutes: i32,
    pub restraint_minutes: i32,
    pub fiscal_year_cumulative_minutes: i32,
    pub fiscal_year_total_minutes: i32,
}

// --- DB row types ---

#[derive(Debug, sqlx::FromRow)]
struct SegmentRow {
    pub work_date: NaiveDate,
    pub unko_no: String,
    pub start_at: chrono::NaiveDateTime,
    pub end_at: chrono::NaiveDateTime,
    pub work_minutes: i32,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
}

#[derive(Debug, sqlx::FromRow)]
struct FiscalCumRow {
    pub total: Option<i64>,
}

async fn get_restraint_report(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Query(filter): Query<RestraintReportFilter>,
) -> Result<Json<RestraintReportResponse>, (StatusCode, String)> {
    let tenant_id = auth_user.tenant_id;
    let driver_id = filter.driver_id;
    let year = filter.year;
    let month = filter.month;

    // Validate month
    let Some(month_start) = NaiveDate::from_ymd_opt(year, month, 1) else {
        return Err((StatusCode::BAD_REQUEST, "invalid year/month".to_string()));
    };
    let month_end = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)
    }
    .unwrap()
        - chrono::Duration::days(1);

    // Get driver name
    let driver_name: String = sqlx::query_scalar(
        "SELECT driver_name FROM drivers WHERE id = $1 AND tenant_id = $2",
    )
    .bind(driver_id)
    .bind(tenant_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(internal_err)?
    .unwrap_or_default();

    // Fetch segments for the month
    let segments = sqlx::query_as::<_, SegmentRow>(
        r#"SELECT work_date, unko_no, start_at, end_at, work_minutes, drive_minutes, cargo_minutes
           FROM daily_work_segments
           WHERE tenant_id = $1 AND driver_id = $2
             AND work_date >= $3 AND work_date <= $4
           ORDER BY work_date, start_at"#,
    )
    .bind(tenant_id)
    .bind(driver_id)
    .bind(month_start)
    .bind(month_end)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_err)?;

    // Fiscal year cumulative (April to previous month)
    let fiscal_year_start = if month >= 4 {
        NaiveDate::from_ymd_opt(year, 4, 1).unwrap()
    } else {
        NaiveDate::from_ymd_opt(year - 1, 4, 1).unwrap()
    };
    let prev_month_end = month_start - chrono::Duration::days(1);

    let fiscal_cum = if fiscal_year_start <= prev_month_end {
        sqlx::query_as::<_, FiscalCumRow>(
            r#"SELECT COALESCE(SUM(total_work_minutes), 0)::BIGINT AS total
               FROM daily_work_hours
               WHERE tenant_id = $1 AND driver_id = $2
                 AND work_date >= $3 AND work_date <= $4"#,
        )
        .bind(tenant_id)
        .bind(driver_id)
        .bind(fiscal_year_start)
        .bind(prev_month_end)
        .fetch_one(&state.pool)
        .await
        .map_err(internal_err)?
        .total
        .unwrap_or(0) as i32
    } else {
        0
    };

    // Group segments by date, then by unko_no
    let mut day_groups: std::collections::BTreeMap<NaiveDate, Vec<&SegmentRow>> =
        std::collections::BTreeMap::new();
    for seg in &segments {
        day_groups.entry(seg.work_date).or_default().push(seg);
    }

    // Build day rows
    let mut days = Vec::new();
    let mut cumulative = 0i32;
    let mut total_drive_so_far = 0i32;
    let mut working_days = 0i32;
    let mut prev_end_at: Option<chrono::NaiveDateTime> = None;

    // Weekly tracking
    let mut weekly_subtotals = Vec::new();
    let mut week_drive = 0i32;
    let mut week_cargo = 0i32;
    let mut week_break = 0i32;
    let mut week_restraint = 0i32;
    let mut current_week_end: Option<NaiveDate> = None;

    let mut current_date = month_start;
    while current_date <= month_end {
        // Check if this is a week boundary (Sunday)
        if current_date.weekday() == chrono::Weekday::Sun && current_date > month_start {
            if week_restraint > 0 {
                weekly_subtotals.push(WeeklySubtotal {
                    week_end_date: current_date - chrono::Duration::days(1),
                    drive_minutes: week_drive,
                    cargo_minutes: week_cargo,
                    break_minutes: week_break,
                    restraint_minutes: week_restraint,
                });
            }
            week_drive = 0;
            week_cargo = 0;
            week_break = 0;
            week_restraint = 0;
        }
        current_week_end = Some(current_date);

        if let Some(segs) = day_groups.get(&current_date) {
            // Group by unko_no
            let mut op_map: std::collections::BTreeMap<&str, (i32, i32, i32, i32)> =
                std::collections::BTreeMap::new();
            let mut day_start: Option<chrono::NaiveDateTime> = None;
            let mut day_end: Option<chrono::NaiveDateTime> = None;

            for seg in segs {
                let entry = op_map.entry(&seg.unko_no).or_insert((0, 0, 0, 0));
                entry.0 += seg.drive_minutes;
                entry.1 += seg.cargo_minutes;
                let seg_break = (seg.work_minutes - seg.drive_minutes - seg.cargo_minutes).max(0);
                entry.2 += seg_break;
                entry.3 += seg.work_minutes;

                day_start = Some(match day_start {
                    Some(s) => s.min(seg.start_at),
                    None => seg.start_at,
                });
                day_end = Some(match day_end {
                    Some(e) => e.max(seg.end_at),
                    None => seg.end_at,
                });
            }

            let operations: Vec<OperationDetail> = op_map
                .iter()
                .map(|(unko_no, (drive, cargo, brk, restraint))| OperationDetail {
                    unko_no: unko_no.to_string(),
                    drive_minutes: *drive,
                    cargo_minutes: *cargo,
                    break_minutes: *brk,
                    restraint_minutes: *restraint,
                })
                .collect();

            let day_drive: i32 = operations.iter().map(|o| o.drive_minutes).sum();
            let day_cargo: i32 = operations.iter().map(|o| o.cargo_minutes).sum();
            let day_restraint: i32 = operations.iter().map(|o| o.restraint_minutes).sum();
            let day_break = (day_restraint - day_drive - day_cargo).max(0);

            cumulative += day_restraint;
            total_drive_so_far += day_drive;
            working_days += 1;
            let drive_avg = total_drive_so_far as f64 / working_days as f64;

            // Rest period: gap from previous work end to current work start
            let rest_period = match (prev_end_at, day_start) {
                (Some(pe), Some(ds)) if ds > pe => {
                    Some((ds - pe).num_minutes() as i32)
                }
                _ => None,
            };

            if let Some(de) = day_end {
                prev_end_at = Some(de);
            }

            week_drive += day_drive;
            week_cargo += day_cargo;
            week_break += day_break;
            week_restraint += day_restraint;

            days.push(RestraintDayRow {
                date: current_date,
                is_holiday: false,
                start_time: day_start.map(|t| t.format("%H:%M").to_string()),
                end_time: day_end.map(|t| t.format("%H:%M").to_string()),
                operations,
                drive_minutes: day_drive,
                cargo_minutes: day_cargo,
                break_minutes: day_break,
                restraint_total_minutes: day_restraint,
                restraint_cumulative_minutes: cumulative,
                drive_average_minutes: (drive_avg * 100.0).round() / 100.0,
                rest_period_minutes: rest_period,
                remarks: String::new(),
            });
        } else {
            // No work on this day (holiday/off)
            days.push(RestraintDayRow {
                date: current_date,
                is_holiday: true,
                start_time: None,
                end_time: None,
                operations: Vec::new(),
                drive_minutes: 0,
                cargo_minutes: 0,
                break_minutes: 0,
                restraint_total_minutes: 0,
                restraint_cumulative_minutes: cumulative,
                drive_average_minutes: if working_days > 0 {
                    ((total_drive_so_far as f64 / working_days as f64) * 100.0).round() / 100.0
                } else {
                    0.0
                },
                rest_period_minutes: None,
                remarks: "休".to_string(),
            });
        }

        current_date += chrono::Duration::days(1);
    }

    // Final weekly subtotal
    if week_restraint > 0 {
        weekly_subtotals.push(WeeklySubtotal {
            week_end_date: current_week_end.unwrap_or(month_end),
            drive_minutes: week_drive,
            cargo_minutes: week_cargo,
            break_minutes: week_break,
            restraint_minutes: week_restraint,
        });
    }

    let monthly_total = MonthlyTotal {
        drive_minutes: total_drive_so_far,
        cargo_minutes: days.iter().map(|d| d.cargo_minutes).sum(),
        break_minutes: days.iter().map(|d| d.break_minutes).sum(),
        restraint_minutes: cumulative,
        fiscal_year_cumulative_minutes: fiscal_cum,
        fiscal_year_total_minutes: fiscal_cum + cumulative,
    };

    // 最大拘束時間: デフォルト275時間（分換算16500）
    let max_restraint_minutes = 275 * 60;

    Ok(Json(RestraintReportResponse {
        driver_id,
        driver_name,
        year,
        month,
        max_restraint_minutes,
        days,
        weekly_subtotals,
        monthly_total,
    }))
}

fn internal_err(e: impl std::fmt::Display) -> (StatusCode, String) {
    tracing::error!("restraint report error: {e}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal server error".to_string(),
    )
}
