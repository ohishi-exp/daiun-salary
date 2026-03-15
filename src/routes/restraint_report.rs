use axum::{
    extract::{Multipart, Query, State},
    http::StatusCode,
    routing::{get, post},
    Extension, Json, Router,
};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::middleware::auth::AuthUser;
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/restraint-report", get(get_restraint_report))
        .route("/restraint-report/compare-csv", post(compare_csv))
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
    pub total_work_minutes: i32,
    pub total_rest_minutes: Option<i32>,
    pub late_night_minutes: i32,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
    pub overlap_drive_minutes: i32,
    pub overlap_cargo_minutes: i32,
    pub overlap_break_minutes: i32,
    pub overlap_restraint_minutes: i32,
    pub ot_late_night_minutes: i32,
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
        r#"SELECT work_date, total_work_minutes, total_rest_minutes, late_night_minutes,
                  drive_minutes, cargo_minutes,
                  overlap_drive_minutes, overlap_cargo_minutes,
                  overlap_break_minutes, overlap_restraint_minutes,
                  ot_late_night_minutes
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

            // daily_work_hours から取得（KUDGIVTイベント直接集計値）
            let dwh = dwh_map.get(&current_date);
            let seg_restraint: i32 = operations.iter().map(|o| o.restraint_minutes).sum();
            let day_drive = dwh.map(|r| r.drive_minutes).unwrap_or_else(|| operations.iter().map(|o| o.drive_minutes).sum());
            let day_cargo = dwh.map(|r| r.cargo_minutes).unwrap_or_else(|| operations.iter().map(|o| o.cargo_minutes).sum());
            let day_restraint = dwh.map(|r| r.total_work_minutes).unwrap_or(seg_restraint);
            let day_break = (day_restraint - day_drive - day_cargo).max(0);
            let overlap_drive = dwh.map(|r| r.overlap_drive_minutes).unwrap_or(0);
            let overlap_cargo = dwh.map(|r| r.overlap_cargo_minutes).unwrap_or(0);
            let overlap_break = dwh.map(|r| r.overlap_break_minutes).unwrap_or(0);
            let overlap_restraint = dwh.map(|r| r.overlap_restraint_minutes).unwrap_or(0);

            // 拘束累計は当日の小計のみで積み上げ（CSV準拠）
            cumulative += day_restraint;

            // 前運転平均: (前日の運転 + 当日の運転) / 2
            let drive_avg_before = prev_main_drive
                .map(|prev| (prev + day_drive) / 2);
            let drive_avg = match prev_main_drive {
                Some(prev) => (prev + day_drive) as f64 / 2.0,
                None => day_drive as f64,
            };

            // 実働時間 = drive + cargo
            let actual_work = day_drive + day_cargo;
            // 時間外深夜（overlap統合時の深夜分）
            let ot_late_night = dwh.map(|r| r.ot_late_night_minutes).unwrap_or(0);
            // 時間外 = max(0, 実働 - 8h) - 時間外深夜
            let total_overtime = (actual_work - 480).max(0);
            let overtime = (total_overtime - ot_late_night).max(0);

            // 休息・深夜（dwh は既に上で取得済み）
            let rest_period = dwh
                .and_then(|r| r.total_rest_minutes)
                .filter(|&v| v > 0);
            let late_night = dwh.map(|r| r.late_night_minutes).unwrap_or(0);

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
                restraint_total_minutes: day_restraint + overlap_restraint,
                restraint_cumulative_minutes: cumulative,
                drive_average_minutes: (drive_avg * 100.0).round() / 100.0,
                rest_period_minutes: rest_period,
                remarks: String::new(),
                overlap_drive_minutes: overlap_drive,
                overlap_cargo_minutes: overlap_cargo,
                overlap_break_minutes: overlap_break,
                overlap_restraint_minutes: overlap_restraint,
                restraint_main_minutes: day_restraint,
                drive_avg_before,
                drive_avg_after: None, // pass 2 で埋める
                actual_work_minutes: actual_work,
                overtime_minutes: overtime,
                late_night_minutes: late_night,
                overtime_late_night_minutes: ot_late_night,
            });
            prev_main_drive = Some(day_drive);
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

// === CSV比較 ===

/// CSV1ドライバー分のパース結果
#[derive(Debug, Serialize)]
pub struct CsvDriverData {
    pub driver_name: String,
    pub driver_cd: String,
    pub days: Vec<CsvDayRow>,
    pub total_drive: String,
    pub total_cargo: String,
    pub total_break: String,
    pub total_restraint: String,
    pub total_actual_work: String,
    pub total_overtime: String,
    pub total_late_night: String,
    pub total_ot_late_night: String,
}

#[derive(Debug, Serialize)]
pub struct CsvDayRow {
    pub date: String,
    pub is_holiday: bool,
    pub start_time: String,
    pub end_time: String,
    pub drive: String,
    pub overlap_drive: String,
    pub cargo: String,
    pub overlap_cargo: String,
    pub break_time: String,
    pub overlap_break: String,
    pub subtotal: String,
    pub overlap_subtotal: String,
    pub total: String,
    pub cumulative: String,
    pub rest: String,
    pub actual_work: String,
    pub overtime: String,
    pub late_night: String,
    pub ot_late_night: String,
    pub remarks: String,
}

#[derive(Debug, Serialize)]
pub struct CompareResult {
    pub driver_name: String,
    pub driver_cd: String,
    pub driver_id: Option<String>,
    pub csv: CsvDriverData,
    pub system: Option<SystemDriverData>,
    pub diffs: Vec<DiffItem>,
}

#[derive(Debug, Serialize)]
pub struct SystemDriverData {
    pub days: Vec<SystemDayRow>,
    pub total_drive: String,
    pub total_overlap_drive: String,
    pub total_restraint: String,
    pub total_actual_work: String,
    pub total_overtime: String,
    pub total_late_night: String,
}

#[derive(Debug, Serialize)]
pub struct SystemDayRow {
    pub date: String,
    pub drive: String,
    pub overlap_drive: String,
    pub cargo: String,
    pub overlap_cargo: String,
    pub subtotal: String,
    pub overlap_subtotal: String,
    pub total: String,
    pub cumulative: String,
    pub actual_work: String,
    pub overtime: String,
    pub late_night: String,
}

#[derive(Debug, Serialize)]
pub struct DiffItem {
    pub date: String,
    pub field: String,
    pub csv_val: String,
    pub sys_val: String,
}

fn fmt_min(val: i32) -> String {
    if val == 0 { return String::new(); }
    format!("{}:{:02}", val / 60, val % 60)
}

fn parse_hhmm(s: &str) -> i32 {
    let s = s.trim();
    if s.is_empty() { return 0; }
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 { return 0; }
    let h: i32 = parts[0].parse().unwrap_or(0);
    let m: i32 = parts[1].parse().unwrap_or(0);
    h * 60 + m
}

/// Shift-JIS/CP932 のCSVをパースして全ドライバーのデータを返す
fn parse_restraint_csv(bytes: &[u8]) -> Result<Vec<CsvDriverData>, String> {
    // CP932 → UTF-8
    let text = if let Ok(s) = String::from_utf8(bytes.to_vec()) {
        s
    } else {
        encoding_rs::SHIFT_JIS.decode(bytes).0.into_owned()
    };

    let mut drivers = Vec::new();
    let mut current: Option<CsvDriverData> = None;
    let mut in_data = false;

    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() { continue; }

        // ドライバーヘッダー検出
        if line.starts_with("氏名,") {
            // 前のドライバーを保存
            if let Some(d) = current.take() {
                drivers.push(d);
            }
            let cols: Vec<&str> = line.split(',').collect();
            let name = cols.get(1).unwrap_or(&"").to_string();
            let cd = cols.get(3).unwrap_or(&"").to_string();
            current = Some(CsvDriverData {
                driver_name: name,
                driver_cd: cd,
                days: Vec::new(),
                total_drive: String::new(),
                total_cargo: String::new(),
                total_break: String::new(),
                total_restraint: String::new(),
                total_actual_work: String::new(),
                total_overtime: String::new(),
                total_late_night: String::new(),
                total_ot_late_night: String::new(),
            });
            in_data = false;
            continue;
        }

        // ヘッダー行をスキップ
        if line.starts_with("日付,") {
            in_data = true;
            continue;
        }

        let Some(ref mut driver) = current else { continue; };
        if !in_data { continue; }

        let cols: Vec<&str> = line.split(',').collect();

        // 合計行
        if cols.first().map(|s| s.contains("合計")).unwrap_or(false) {
            driver.total_drive = cols.get(3).unwrap_or(&"").to_string();
            driver.total_cargo = cols.get(5).unwrap_or(&"").to_string();
            driver.total_break = cols.get(7).unwrap_or(&"").to_string();
            driver.total_restraint = cols.get(11).unwrap_or(&"").to_string();
            driver.total_actual_work = cols.get(17).unwrap_or(&"").to_string();
            driver.total_overtime = cols.get(18).unwrap_or(&"").to_string();
            driver.total_late_night = cols.get(19).unwrap_or(&"").to_string();
            driver.total_ot_late_night = cols.get(20).unwrap_or(&"").to_string();
            in_data = false;
            continue;
        }

        // 日付行チェック（N月N日）
        let date_str = cols.first().unwrap_or(&"").to_string();
        if !date_str.contains('月') { continue; }

        let is_holiday = cols.get(1).map(|s| s.trim() == "休").unwrap_or(false);

        driver.days.push(CsvDayRow {
            date: date_str,
            is_holiday,
            start_time: cols.get(1).unwrap_or(&"").to_string(),
            end_time: cols.get(2).unwrap_or(&"").to_string(),
            drive: cols.get(3).unwrap_or(&"").to_string(),
            overlap_drive: cols.get(4).unwrap_or(&"").to_string(),
            cargo: cols.get(5).unwrap_or(&"").to_string(),
            overlap_cargo: cols.get(6).unwrap_or(&"").to_string(),
            break_time: cols.get(7).unwrap_or(&"").to_string(),
            overlap_break: cols.get(8).unwrap_or(&"").to_string(),
            subtotal: cols.get(11).unwrap_or(&"").to_string(),
            overlap_subtotal: cols.get(12).unwrap_or(&"").to_string(),
            total: cols.get(13).unwrap_or(&"").to_string(),
            cumulative: cols.get(14).unwrap_or(&"").to_string(),
            rest: cols.get(17).unwrap_or(&"").to_string(),
            actual_work: cols.get(18).unwrap_or(&"").to_string(),
            overtime: cols.get(19).unwrap_or(&"").to_string(),
            late_night: cols.get(20).unwrap_or(&"").to_string(),
            ot_late_night: cols.get(21).unwrap_or(&"").to_string(),
            remarks: cols.get(22).unwrap_or(&"").to_string(),
        });
    }

    if let Some(d) = current {
        drivers.push(d);
    }

    Ok(drivers)
}

async fn compare_csv(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    mut multipart: Multipart,
) -> Result<Json<Vec<CompareResult>>, (StatusCode, String)> {
    let tenant_id = auth_user.tenant_id;

    // CSVファイルを受け取る
    let mut csv_bytes = Vec::new();
    while let Some(field) = multipart.next_field().await.map_err(|e| {
        (StatusCode::BAD_REQUEST, format!("multipart error: {e}"))
    })? {
        if let Some(data) = field.bytes().await.ok() {
            csv_bytes = data.to_vec();
            break;
        }
    }

    if csv_bytes.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "CSVファイルが空です".to_string()));
    }

    let csv_drivers = parse_restraint_csv(&csv_bytes)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    // 年月をCSVの日付行から推測
    let (year, month) = csv_drivers.first()
        .and_then(|d| d.days.first())
        .and_then(|day| {
            // "2月1日" → month=2
            let s = &day.date;
            let m_pos = s.find('月')?;
            let m: u32 = s[..m_pos].parse().ok()?;
            Some(m)
        })
        .map(|m| {
            // 年はCSVヘッダーから取れないので現在年を使う（要改善）
            (2026i32, m)
        })
        .unwrap_or((2026, 1));

    // 全ドライバー取得
    let db_drivers: Vec<(Uuid, String, String)> = sqlx::query_as(
        "SELECT id, driver_cd, driver_name FROM drivers WHERE tenant_id = $1",
    )
    .bind(tenant_id)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_err)?;

    let mut results = Vec::new();

    for csv_d in &csv_drivers {
        // driver_cd でマッチ
        let db_match = db_drivers.iter().find(|(_, cd, _)| cd == &csv_d.driver_cd);

        let (driver_id, system_data, diffs) = if let Some((did, _, dname)) = db_match {
            // システムのレポートを取得
            match build_report_with_name(&state.pool, tenant_id, *did, dname, year, month).await {
                Ok(report) => {
                    let sys_days: Vec<SystemDayRow> = report.days.iter().map(|d| SystemDayRow {
                        date: format!("{}月{}日", d.date.month(), d.date.day()),
                        drive: fmt_min(d.drive_minutes),
                        overlap_drive: fmt_min(d.overlap_drive_minutes),
                        cargo: fmt_min(d.cargo_minutes),
                        overlap_cargo: fmt_min(d.overlap_cargo_minutes),
                        subtotal: fmt_min(d.restraint_main_minutes),
                        overlap_subtotal: fmt_min(d.overlap_restraint_minutes),
                        total: fmt_min(d.restraint_total_minutes),
                        cumulative: fmt_min(d.restraint_cumulative_minutes),
                        actual_work: fmt_min(d.actual_work_minutes),
                        overtime: fmt_min(d.overtime_minutes),
                        late_night: fmt_min(d.late_night_minutes),
                    }).collect();

                    // 差分検出
                    let mut diffs = Vec::new();
                    for (csv_day, sys_day) in csv_d.days.iter().zip(sys_days.iter()) {
                        if csv_day.is_holiday { continue; }
                        let checks = [
                            ("運転", &csv_day.drive, &sys_day.drive),
                            ("重複運転", &csv_day.overlap_drive, &sys_day.overlap_drive),
                            ("小計", &csv_day.subtotal, &sys_day.subtotal),
                            ("重複小計", &csv_day.overlap_subtotal, &sys_day.overlap_subtotal),
                            ("合計", &csv_day.total, &sys_day.total),
                            ("累計", &csv_day.cumulative, &sys_day.cumulative),
                            ("実働", &csv_day.actual_work, &sys_day.actual_work),
                            ("時間外", &csv_day.overtime, &sys_day.overtime),
                            ("深夜", &csv_day.late_night, &sys_day.late_night),
                        ];
                        for (field, csv_val, sys_val) in checks {
                            let cv = csv_val.trim();
                            let sv = sys_val.trim();
                            if cv != sv && !(cv.is_empty() && sv.is_empty()) {
                                diffs.push(DiffItem {
                                    date: csv_day.date.clone(),
                                    field: field.to_string(),
                                    csv_val: cv.to_string(),
                                    sys_val: sv.to_string(),
                                });
                            }
                        }
                    }

                    let sys_data = SystemDriverData {
                        days: sys_days,
                        total_drive: fmt_min(report.monthly_total.drive_minutes),
                        total_overlap_drive: fmt_min(report.monthly_total.overlap_drive_minutes),
                        total_restraint: fmt_min(report.monthly_total.restraint_minutes),
                        total_actual_work: fmt_min(report.monthly_total.actual_work_minutes),
                        total_overtime: fmt_min(report.monthly_total.overtime_minutes),
                        total_late_night: fmt_min(report.monthly_total.late_night_minutes),
                    };

                    (Some(did.to_string()), Some(sys_data), diffs)
                }
                Err(_) => (Some(did.to_string()), None, Vec::new()),
            }
        } else {
            (None, None, Vec::new())
        };

        results.push(CompareResult {
            driver_name: csv_d.driver_name.clone(),
            driver_cd: csv_d.driver_cd.clone(),
            driver_id,
            csv: CsvDriverData {
                driver_name: csv_d.driver_name.clone(),
                driver_cd: csv_d.driver_cd.clone(),
                days: csv_d.days.iter().map(|d| CsvDayRow {
                    date: d.date.clone(),
                    is_holiday: d.is_holiday,
                    start_time: d.start_time.clone(),
                    end_time: d.end_time.clone(),
                    drive: d.drive.clone(),
                    overlap_drive: d.overlap_drive.clone(),
                    cargo: d.cargo.clone(),
                    overlap_cargo: d.overlap_cargo.clone(),
                    break_time: d.break_time.clone(),
                    overlap_break: d.overlap_break.clone(),
                    subtotal: d.subtotal.clone(),
                    overlap_subtotal: d.overlap_subtotal.clone(),
                    total: d.total.clone(),
                    cumulative: d.cumulative.clone(),
                    rest: d.rest.clone(),
                    actual_work: d.actual_work.clone(),
                    overtime: d.overtime.clone(),
                    late_night: d.late_night.clone(),
                    ot_late_night: d.ot_late_night.clone(),
                    remarks: d.remarks.clone(),
                }).collect(),
                total_drive: csv_d.total_drive.clone(),
                total_cargo: csv_d.total_cargo.clone(),
                total_break: csv_d.total_break.clone(),
                total_restraint: csv_d.total_restraint.clone(),
                total_actual_work: csv_d.total_actual_work.clone(),
                total_overtime: csv_d.total_overtime.clone(),
                total_late_night: csv_d.total_late_night.clone(),
                total_ot_late_night: csv_d.total_ot_late_night.clone(),
            },
            system: system_data,
            diffs,
        });
    }

    Ok(Json(results))
}
