use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Extension, Json, Router,
};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
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
    // CSV互換フィールド
    pub overlap_drive_minutes: i32,
    pub overlap_cargo_minutes: i32,
    pub overlap_break_minutes: i32,
    pub overlap_restraint_minutes: i32,
    pub restraint_main_minutes: i32,
    pub drive_avg_before: Option<i32>,
    pub drive_avg_after: Option<i32>,
    pub actual_work_minutes: i32,
    pub overtime_minutes: i32,
    pub late_night_minutes: i32,
    pub overtime_late_night_minutes: i32,
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
    // CSV互換フィールド
    pub overlap_drive_minutes: i32,
    pub overlap_cargo_minutes: i32,
    pub overlap_break_minutes: i32,
    pub overlap_restraint_minutes: i32,
    pub actual_work_minutes: i32,
    pub overtime_minutes: i32,
    pub late_night_minutes: i32,
    pub overtime_late_night_minutes: i32,
}

// --- DB row types ---

#[derive(Debug, sqlx::FromRow)]
struct SegmentRow {
    pub work_date: NaiveDate,
    pub unko_no: String,
    pub start_at: DateTime<Utc>,
    pub end_at: DateTime<Utc>,
    pub work_minutes: i32,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
}

#[derive(Debug, sqlx::FromRow)]
struct FiscalCumRow {
    pub total: Option<i64>,
}

#[derive(Debug, sqlx::FromRow)]
struct DailyWorkHoursRow {
    pub work_date: NaiveDate,
    pub total_rest_minutes: Option<i32>,
    pub late_night_minutes: i32,
}

async fn get_restraint_report(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Query(filter): Query<RestraintReportFilter>,
) -> Result<Json<RestraintReportResponse>, (StatusCode, String)> {
    let report = build_report(
        &state.pool,
        auth_user.tenant_id,
        filter.driver_id,
        filter.year,
        filter.month,
    )
    .await?;
    Ok(Json(report))
}

pub async fn build_report(
    pool: &sqlx::PgPool,
    tenant_id: Uuid,
    driver_id: Uuid,
    year: i32,
    month: u32,
) -> Result<RestraintReportResponse, (StatusCode, String)> {
    // Get driver name
    let driver_name: String = sqlx::query_scalar(
        "SELECT driver_name FROM drivers WHERE id = $1 AND tenant_id = $2",
    )
    .bind(driver_id)
    .bind(tenant_id)
    .fetch_optional(pool)
    .await
    .map_err(internal_err)?
    .unwrap_or_default();

    build_report_with_name(pool, tenant_id, driver_id, &driver_name, year, month).await
}

pub async fn build_report_with_name(
    pool: &sqlx::PgPool,
    tenant_id: Uuid,
    driver_id: Uuid,
    driver_name: &str,
    year: i32,
    month: u32,
) -> Result<RestraintReportResponse, (StatusCode, String)> {
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
    .fetch_all(pool)
    .await
    .map_err(internal_err)?;

    // Fetch daily_work_hours for the month (batch query instead of per-day)
    let dwh_rows = sqlx::query_as::<_, DailyWorkHoursRow>(
        r#"SELECT work_date, total_rest_minutes, late_night_minutes
           FROM daily_work_hours
           WHERE tenant_id = $1 AND driver_id = $2
             AND work_date >= $3 AND work_date <= $4"#,
    )
    .bind(tenant_id)
    .bind(driver_id)
    .bind(month_start)
    .bind(month_end)
    .fetch_all(pool)
    .await
    .map_err(internal_err)?;

    let dwh_map: std::collections::HashMap<NaiveDate, &DailyWorkHoursRow> =
        dwh_rows.iter().map(|r| (r.work_date, r)).collect();

    // Fetch previous day's drive minutes (for 前運転平均 on day 1)
    let prev_day = month_start - chrono::Duration::days(1);
    let prev_day_main_drive: Option<i32> = sqlx::query_scalar(
        r#"SELECT drive_minutes FROM daily_work_segments
           WHERE tenant_id = $1 AND driver_id = $2 AND work_date = $3
           ORDER BY start_at LIMIT 1"#,
    )
    .bind(tenant_id)
    .bind(driver_id)
    .bind(prev_day)
    .fetch_optional(pool)
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
        .fetch_one(pool)
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

    // Build day rows (pass 1)
    let mut days = Vec::new();
    let mut cumulative = 0i32;
    let mut prev_main_drive: Option<i32> = prev_day_main_drive;

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
            let mut day_start: Option<DateTime<Utc>> = None;
            let mut day_end: Option<DateTime<Utc>> = None;

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

            // 主運行 = 最初の運行, 重複 = 2番目以降
            let main_op = &operations[0];
            let main_drive = main_op.drive_minutes;
            let main_cargo = main_op.cargo_minutes;
            let main_break = main_op.break_minutes;
            let main_restraint = main_op.restraint_minutes;

            let overlap_drive: i32 = operations.iter().skip(1).map(|o| o.drive_minutes).sum();
            let overlap_cargo: i32 = operations.iter().skip(1).map(|o| o.cargo_minutes).sum();
            let overlap_break: i32 = operations.iter().skip(1).map(|o| o.break_minutes).sum();
            let overlap_restraint: i32 = operations.iter().skip(1).map(|o| o.restraint_minutes).sum();

            let day_drive = main_drive + overlap_drive;
            let day_cargo = main_cargo + overlap_cargo;
            let day_restraint = main_restraint + overlap_restraint;
            let day_break = (day_restraint - day_drive - day_cargo).max(0);

            // 拘束累計は主運行の小計のみで積み上げ（CSV準拠）
            cumulative += main_restraint;

            // 前運転平均: (前日の主運転 + 当日の主運転) / 2
            let drive_avg_before = prev_main_drive
                .map(|prev| (prev + main_drive) / 2);
            // drive_average_minutes は互換性のため残す
            let drive_avg = match prev_main_drive {
                Some(prev) => (prev + main_drive) as f64 / 2.0,
                None => main_drive as f64,
            };

            // 実働時間 = drive + cargo（全運行合算）
            let actual_work = day_drive + day_cargo;
            // 時間外 = max(0, 実働 - 8h)
            let overtime = (actual_work - 480).max(0);

            // daily_work_hours から休息・深夜を取得
            let dwh = dwh_map.get(&current_date);
            let rest_period = dwh
                .and_then(|r| r.total_rest_minutes)
                .filter(|&v| v > 0);
            let late_night = dwh.map(|r| r.late_night_minutes).unwrap_or(0);

            week_drive += day_drive;
            week_cargo += day_cargo;
            week_break += day_break;
            week_restraint += main_restraint;

            days.push(RestraintDayRow {
                date: current_date,
                is_holiday: false,
                start_time: day_start.map(|t| t.format("%H:%M").to_string()),
                end_time: day_end.map(|t| t.format("%H:%M").to_string()),
                operations,
                drive_minutes: main_drive,
                cargo_minutes: main_cargo,
                break_minutes: main_break,
                restraint_total_minutes: day_restraint,
                restraint_cumulative_minutes: cumulative,
                drive_average_minutes: (drive_avg * 100.0).round() / 100.0,
                rest_period_minutes: rest_period,
                remarks: String::new(),
                overlap_drive_minutes: overlap_drive,
                overlap_cargo_minutes: overlap_cargo,
                overlap_break_minutes: overlap_break,
                overlap_restraint_minutes: overlap_restraint,
                restraint_main_minutes: main_restraint,
                drive_avg_before: drive_avg_before,
                drive_avg_after: None, // pass 2 で埋める
                actual_work_minutes: actual_work,
                overtime_minutes: overtime,
                late_night_minutes: late_night,
                overtime_late_night_minutes: 0,
            });
            prev_main_drive = Some(main_drive);
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
                drive_average_minutes: 0.0,
                rest_period_minutes: None,
                remarks: "休".to_string(),
                overlap_drive_minutes: 0,
                overlap_cargo_minutes: 0,
                overlap_break_minutes: 0,
                overlap_restraint_minutes: 0,
                restraint_main_minutes: 0,
                drive_avg_before: None,
                drive_avg_after: None,
                actual_work_minutes: 0,
                overtime_minutes: 0,
                late_night_minutes: 0,
                overtime_late_night_minutes: 0,
            });
            // 休日の場合、prev_main_drive は 0 として扱う（CSV準拠）
            prev_main_drive = Some(0);
        }

        current_date += chrono::Duration::days(1);
    }

    // Pass 2: 後運転平均を埋める（当日の主運転 + 翌日の主運転）/ 2
    for i in 0..days.len() {
        if days[i].is_holiday {
            continue;
        }
        let current_main_drive = days[i].drive_minutes;
        let next_main_drive = if i + 1 < days.len() {
            days[i + 1].drive_minutes // 休日なら0
        } else {
            0
        };
        days[i].drive_avg_after = Some((current_main_drive + next_main_drive) / 2);
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
        drive_minutes: days.iter().map(|d| d.drive_minutes).sum(),
        cargo_minutes: days.iter().map(|d| d.cargo_minutes).sum(),
        break_minutes: days.iter().map(|d| d.break_minutes).sum(),
        restraint_minutes: cumulative,
        fiscal_year_cumulative_minutes: fiscal_cum,
        fiscal_year_total_minutes: fiscal_cum + cumulative,
        overlap_drive_minutes: days.iter().map(|d| d.overlap_drive_minutes).sum(),
        overlap_cargo_minutes: days.iter().map(|d| d.overlap_cargo_minutes).sum(),
        overlap_break_minutes: days.iter().map(|d| d.overlap_break_minutes).sum(),
        overlap_restraint_minutes: days.iter().map(|d| d.overlap_restraint_minutes).sum(),
        actual_work_minutes: days.iter().map(|d| d.actual_work_minutes).sum(),
        overtime_minutes: days.iter().map(|d| d.overtime_minutes).sum(),
        late_night_minutes: days.iter().map(|d| d.late_night_minutes).sum(),
        overtime_late_night_minutes: 0,
    };

    // 最大拘束時間: デフォルト275時間（分換算16500）
    let max_restraint_minutes = 275 * 60;

    Ok(RestraintReportResponse {
        driver_id,
        driver_name: driver_name.to_string(),
        year,
        month,
        max_restraint_minutes,
        days,
        weekly_subtotals,
        monthly_total,
    })
}

fn internal_err(e: impl std::fmt::Display) -> (StatusCode, String) {
    tracing::error!("restraint report error: {e}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal server error".to_string(),
    )
}
