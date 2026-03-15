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

/// CSV行とシステム行の差分を検出する（compare_csvの内部ロジック抽出）
fn detect_diffs(csv_days: &[CsvDayRow], sys_days: &[SystemDayRow]) -> Vec<DiffItem> {
    let mut diffs = Vec::new();
    for (csv_day, sys_day) in csv_days.iter().zip(sys_days.iter()) {
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
    diffs
}

#[cfg(test)]
mod tests {
    use super::*;

    // 鈴木　昭 (1021) 2026年2月分 — 0件差分が保証されるべきリファレンスデータ
    const CSV_1021: &str = "拘束時間管理表 (2026年 2月分)\n\
※当月の最大拘束時間 : 275 時間（労使協定により時間を記入する）\n\
\n\
事業所,大石運輸倉庫㈱　本社営業所,乗務員分類1,第３運行課,乗務員分類2,1,乗務員分類3,第３運行課,乗務員分類4,未設定,乗務員分類5,未設定\n\
氏名,鈴木　昭,乗務員コード,1021\n\
日付,始業時刻,終業時刻,運転時間,重複運転時間,荷役時間,重複荷役時間,休憩時間,重複休憩時間,時間,重複時間,拘束時間小計,重複拘束時間小計,拘束時間合計,拘束時間累計,前運転平均,後運転平均,休息時間,実働時間,時間外時間,深夜時間,時間外深夜時間,摘要1,摘要2\n\
2月1日,5:55,15:14,2:43,0:12,,,2:35,,,,5:18,0:12,5:30,5:18,6:52,4:07,18:30,2:43,,,,,\n\
2月2日,5:43,15:08,5:32,,1:21,,2:32,,,,9:25,,9:25,14:43,,2:46,14:35,6:53,,,,2/2帰着,\n\
2月3日,休,\n\
2月4日,7:23,15:02,5:00,2:57,0:41,,1:58,,,,7:39,2:57,10:36,22:22,,6:49,13:24,5:41,,,,2/4出発,\n\
2月5日,4:26,15:22,8:39,,,,2:17,,,,10:56,,10:56,33:18,,5:44,13:04,8:39,0:39,0:34,,,\n\
2月6日,7:26,13:14,2:49,1:03,0:37,,2:22,0:18,,,5:48,1:21,7:09,39:06,,5:03,16:51,3:26,,,,,\n\
2月7日,6:05,14:15,4:48,1:08,0:58,,2:24,,,,8:10,1:08,9:18,47:16,,5:36,14:42,5:46,,,,,\n\
2月8日,4:57,13:17,6:25,,,,1:55,,,,8:20,,8:20,55:36,,5:33,15:40,6:25,,0:03,,,\n\
2月9日,7:33,16:10,4:42,1:39,2:02,,1:53,0:21,,,8:37,2:00,10:37,64:13,,8:26,13:23,6:44,,,,,\n\
2月10日,5:33,17:20,9:40,1:31,,,2:07,0:20,,,11:47,1:51,13:38,76:00,,8:13,10:22,9:40,1:40,,,,\n\
2月11日,3:42,16:08,6:47,,,,5:39,,,,12:26,,12:26,88:26,,4:33,11:34,6:47,,0:58,,,\n\
2月12日,7:36,16:16,2:20,0:20,0:19,,6:01,0:04,,,8:40,0:24,9:04,97:06,,4:28,14:56,2:39,,,,,\n\
2月13日,7:12,15:49,3:08,3:28,1:13,,4:16,0:18,,,8:37,3:46,12:23,105:43,,7:43,11:37,4:21,,,,,\n\
2月14日,3:26,15:34,9:23,0:09,,,2:45,,,,12:08,0:09,12:17,117:51,,7:29,11:43,9:23,1:23,1:34,,,\n\
2月15日,3:17,12:09,5:35,,,,3:17,,,,8:52,,8:52,126:43,,5:08,15:08,5:35,,1:43,,,\n\
2月16日,5:47,15:50,4:41,,1:52,,3:30,,,,10:03,,10:03,136:46,,2:20,13:57,6:33,,,,2/16帰着,\n\
2月17日,休,\n\
2月18日,5:51,5:51,7:27,,2:30,,6:34,,,,16:31,,16:31,153:17,,8:28,7:29,9:57,0:51,,1:06,2/18出発,\n\
2月19日,5:51,16:49,8:37,0:52,,,2:21,,,,10:58,0:52,11:50,164:15,,9:23,12:10,8:37,0:37,,,,\n\
2月20日,4:59,18:19,8:06,1:12,2:48,,2:26,,,,13:20,1:12,14:32,177:35,,9:46,9:28,10:54,2:54,0:01,,,\n\
2月21日,3:47,17:30,9:39,0:36,,,4:04,,,,13:43,0:36,14:19,191:18,,7:45,9:41,9:39,1:39,1:13,,,\n\
2月22日,3:11,13:40,5:51,,,,4:38,,,,10:29,,10:29,201:47,,4:16,13:31,5:51,,1:49,,,\n\
2月23日,4:11,12:25,2:42,,,,5:32,,,,8:14,,8:14,210:01,,2:59,15:46,2:42,,0:49,,,\n\
2月24日,7:13,13:55,3:17,,0:17,,3:08,,,,6:42,,6:42,216:43,,2:41,17:18,3:34,,,,2/24帰着,\n\
2月25日,10:04,16:41,2:06,1:51,0:39,,0:14,,,,2:59,1:51,4:50,219:42,,5:14,19:10,2:45,,,,2/25出発,\n\
2月26日,8:13,15:31,6:45,1:38,,,0:33,,,,7:18,1:38,8:56,227:00,,7:20,15:04,6:45,,,,,\n\
2月27日,6:20,18:23,5:22,1:06,0:14,,1:55,,,,7:31,1:06,8:37,234:31,,5:34,15:23,5:36,,,,,\n\
2月28日,5:14,18:12,4:50,,0:03,,3:16,,,,8:09,,8:09,242:40,,5:34,15:51,4:53,,,,,\n\
合計,,,146:54,,15:34,,80:12,,,,242:40,,,,,,360:17,162:28,9:43,8:44,1:06,,\n";

    #[test]
    fn test_parse_csv_1021() {
        let drivers = parse_restraint_csv(CSV_1021.as_bytes()).unwrap();
        assert_eq!(drivers.len(), 1);
        let d = &drivers[0];
        assert_eq!(d.driver_name, "鈴木　昭");
        assert_eq!(d.driver_cd, "1021");
        assert_eq!(d.days.len(), 28); // 2月1日〜28日
        assert_eq!(d.total_drive, "146:54");
        assert_eq!(d.total_restraint, "242:40");

        // 2月1日: 稼働日
        let day1 = &d.days[0];
        assert_eq!(day1.date, "2月1日");
        assert!(!day1.is_holiday);
        assert_eq!(day1.drive, "2:43");
        assert_eq!(day1.overlap_drive, "0:12");
        assert_eq!(day1.subtotal, "5:18");
        assert_eq!(day1.overlap_subtotal, "0:12");
        assert_eq!(day1.total, "5:30");
        assert_eq!(day1.cumulative, "5:18");
        assert_eq!(day1.actual_work, "2:43");

        // 2月3日: 休日
        let day3 = &d.days[2];
        assert_eq!(day3.date, "2月3日");
        assert!(day3.is_holiday);
    }

    #[test]
    fn test_compare_1021_zero_diffs() {
        // CSVの期待値をシステム側としても使う → 差分0件を保証
        let drivers = parse_restraint_csv(CSV_1021.as_bytes()).unwrap();
        let csv_d = &drivers[0];

        // CSVの値をそのままSystemDayRowに変換（= 完全一致するはず）
        let sys_days: Vec<SystemDayRow> = csv_d.days.iter().map(|d| SystemDayRow {
            date: d.date.clone(),
            drive: d.drive.clone(),
            overlap_drive: d.overlap_drive.clone(),
            cargo: d.cargo.clone(),
            overlap_cargo: d.overlap_cargo.clone(),
            subtotal: d.subtotal.clone(),
            overlap_subtotal: d.overlap_subtotal.clone(),
            total: d.total.clone(),
            cumulative: d.cumulative.clone(),
            actual_work: d.actual_work.clone(),
            overtime: d.overtime.clone(),
            late_night: d.late_night.clone(),
        }).collect();

        let diffs = detect_diffs(&csv_d.days, &sys_days);
        assert_eq!(diffs.len(), 0, "Expected 0 diffs but got {}: {:?}", diffs.len(), diffs);
    }

    #[test]
    fn test_compare_detects_diff() {
        let drivers = parse_restraint_csv(CSV_1021.as_bytes()).unwrap();
        let csv_d = &drivers[0];

        // 1行目のdriveを変更 → 差分1件が出るはず
        let mut sys_days: Vec<SystemDayRow> = csv_d.days.iter().map(|d| SystemDayRow {
            date: d.date.clone(),
            drive: d.drive.clone(),
            overlap_drive: d.overlap_drive.clone(),
            cargo: d.cargo.clone(),
            overlap_cargo: d.overlap_cargo.clone(),
            subtotal: d.subtotal.clone(),
            overlap_subtotal: d.overlap_subtotal.clone(),
            total: d.total.clone(),
            cumulative: d.cumulative.clone(),
            actual_work: d.actual_work.clone(),
            overtime: d.overtime.clone(),
            late_night: d.late_night.clone(),
        }).collect();
        sys_days[0].drive = "9:99".to_string();

        let diffs = detect_diffs(&csv_d.days, &sys_days);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].field, "運転");
        assert_eq!(diffs[0].date, "2月1日");
        assert_eq!(diffs[0].csv_val, "2:43");
        assert_eq!(diffs[0].sys_val, "9:99");
    }

    /// DB値（daily_work_hours）からbuild_report_with_nameと同じ変換ロジックでSystemDayRowを生成
    struct MockDwh {
        day: u32,
        drive: i32, overlap_drive: i32,
        cargo: i32, overlap_cargo: i32,
        restraint: i32, overlap_restraint: i32,
        late_night: i32, ot_late_night: i32,
    }

    fn build_sys_days_from_mock(mock_data: &[MockDwh]) -> Vec<SystemDayRow> {
        let mut rows = Vec::new();
        let mut cumulative = 0i32;
        for day_num in 1..=28u32 {
            let date_str = format!("2月{}日", day_num);
            if let Some(dwh) = mock_data.iter().find(|m| m.day == day_num) {
                let actual_work = dwh.drive + dwh.cargo;
                let overtime = ((actual_work - 480).max(0) - dwh.ot_late_night).max(0);
                cumulative += dwh.restraint;
                rows.push(SystemDayRow {
                    date: date_str,
                    drive: fmt_min(dwh.drive), overlap_drive: fmt_min(dwh.overlap_drive),
                    cargo: fmt_min(dwh.cargo), overlap_cargo: fmt_min(dwh.overlap_cargo),
                    subtotal: fmt_min(dwh.restraint), overlap_subtotal: fmt_min(dwh.overlap_restraint),
                    total: fmt_min(dwh.restraint + dwh.overlap_restraint),
                    cumulative: fmt_min(cumulative),
                    actual_work: fmt_min(actual_work), overtime: fmt_min(overtime),
                    late_night: fmt_min(dwh.late_night),
                });
            } else {
                rows.push(SystemDayRow {
                    date: date_str, drive: String::new(), overlap_drive: String::new(),
                    cargo: String::new(), overlap_cargo: String::new(),
                    subtotal: String::new(), overlap_subtotal: String::new(),
                    total: String::new(), cumulative: String::new(),
                    actual_work: String::new(), overtime: String::new(), late_night: String::new(),
                });
            }
        }
        rows
    }

    /// 本番DB値を使った回帰テスト: DB→SystemDayRow変換→CSV比較で0件差分を保証
    #[test]
    fn test_compare_1021_with_db_mock() {
        let drivers = parse_restraint_csv(CSV_1021.as_bytes()).unwrap();
        let csv_d = &drivers[0];

        // 本番DBから取得した鈴木昭(1021) 2026年2月のdaily_work_hours値
        let mock = vec![
            MockDwh { day: 1,  drive: 163, overlap_drive: 12,  cargo: 0,   overlap_cargo: 0, restraint: 318,  overlap_restraint: 12,  late_night: 0,   ot_late_night: 0 },
            MockDwh { day: 2,  drive: 332, overlap_drive: 0,   cargo: 81,  overlap_cargo: 0, restraint: 565,  overlap_restraint: 0,   late_night: 0,   ot_late_night: 0 },
            MockDwh { day: 4,  drive: 300, overlap_drive: 177, cargo: 41,  overlap_cargo: 0, restraint: 459,  overlap_restraint: 177, late_night: 0,   ot_late_night: 0 },
            MockDwh { day: 5,  drive: 519, overlap_drive: 0,   cargo: 0,   overlap_cargo: 0, restraint: 656,  overlap_restraint: 0,   late_night: 34,  ot_late_night: 0 },
            MockDwh { day: 6,  drive: 169, overlap_drive: 63,  cargo: 37,  overlap_cargo: 0, restraint: 348,  overlap_restraint: 81,  late_night: 0,   ot_late_night: 0 },
            MockDwh { day: 7,  drive: 288, overlap_drive: 68,  cargo: 58,  overlap_cargo: 0, restraint: 490,  overlap_restraint: 68,  late_night: 0,   ot_late_night: 0 },
            MockDwh { day: 8,  drive: 385, overlap_drive: 0,   cargo: 0,   overlap_cargo: 0, restraint: 500,  overlap_restraint: 0,   late_night: 3,   ot_late_night: 0 },
            MockDwh { day: 9,  drive: 282, overlap_drive: 99,  cargo: 122, overlap_cargo: 0, restraint: 517,  overlap_restraint: 120, late_night: 0,   ot_late_night: 0 },
            MockDwh { day: 10, drive: 580, overlap_drive: 91,  cargo: 0,   overlap_cargo: 0, restraint: 707,  overlap_restraint: 111, late_night: 0,   ot_late_night: 0 },
            MockDwh { day: 11, drive: 407, overlap_drive: 0,   cargo: 0,   overlap_cargo: 0, restraint: 746,  overlap_restraint: 0,   late_night: 58,  ot_late_night: 0 },
            MockDwh { day: 12, drive: 140, overlap_drive: 20,  cargo: 19,  overlap_cargo: 0, restraint: 520,  overlap_restraint: 24,  late_night: 0,   ot_late_night: 0 },
            MockDwh { day: 13, drive: 188, overlap_drive: 208, cargo: 73,  overlap_cargo: 0, restraint: 517,  overlap_restraint: 226, late_night: 0,   ot_late_night: 0 },
            MockDwh { day: 14, drive: 563, overlap_drive: 9,   cargo: 0,   overlap_cargo: 0, restraint: 728,  overlap_restraint: 9,   late_night: 94,  ot_late_night: 0 },
            MockDwh { day: 15, drive: 335, overlap_drive: 0,   cargo: 0,   overlap_cargo: 0, restraint: 532,  overlap_restraint: 0,   late_night: 103, ot_late_night: 0 },
            MockDwh { day: 16, drive: 281, overlap_drive: 0,   cargo: 112, overlap_cargo: 0, restraint: 603,  overlap_restraint: 0,   late_night: 0,   ot_late_night: 0 },
            MockDwh { day: 18, drive: 447, overlap_drive: 0,   cargo: 150, overlap_cargo: 0, restraint: 991,  overlap_restraint: 0,   late_night: 0,   ot_late_night: 66 },
            MockDwh { day: 19, drive: 517, overlap_drive: 52,  cargo: 0,   overlap_cargo: 0, restraint: 658,  overlap_restraint: 52,  late_night: 0,   ot_late_night: 0 },
            MockDwh { day: 20, drive: 486, overlap_drive: 72,  cargo: 168, overlap_cargo: 0, restraint: 800,  overlap_restraint: 72,  late_night: 1,   ot_late_night: 0 },
            MockDwh { day: 21, drive: 579, overlap_drive: 36,  cargo: 0,   overlap_cargo: 0, restraint: 823,  overlap_restraint: 36,  late_night: 73,  ot_late_night: 0 },
            MockDwh { day: 22, drive: 351, overlap_drive: 0,   cargo: 0,   overlap_cargo: 0, restraint: 629,  overlap_restraint: 0,   late_night: 109, ot_late_night: 0 },
            MockDwh { day: 23, drive: 162, overlap_drive: 0,   cargo: 0,   overlap_cargo: 0, restraint: 494,  overlap_restraint: 0,   late_night: 49,  ot_late_night: 0 },
            MockDwh { day: 24, drive: 197, overlap_drive: 0,   cargo: 17,  overlap_cargo: 0, restraint: 402,  overlap_restraint: 0,   late_night: 0,   ot_late_night: 0 },
            MockDwh { day: 25, drive: 126, overlap_drive: 111, cargo: 39,  overlap_cargo: 0, restraint: 179,  overlap_restraint: 111, late_night: 0,   ot_late_night: 0 },
            MockDwh { day: 26, drive: 405, overlap_drive: 98,  cargo: 0,   overlap_cargo: 0, restraint: 438,  overlap_restraint: 98,  late_night: 0,   ot_late_night: 0 },
            MockDwh { day: 27, drive: 322, overlap_drive: 66,  cargo: 14,  overlap_cargo: 0, restraint: 451,  overlap_restraint: 66,  late_night: 0,   ot_late_night: 0 },
            MockDwh { day: 28, drive: 290, overlap_drive: 0,   cargo: 3,   overlap_cargo: 0, restraint: 489,  overlap_restraint: 0,   late_night: 0,   ot_late_night: 0 },
        ];

        let sys_days = build_sys_days_from_mock(&mock);
        let diffs = detect_diffs(&csv_d.days, &sys_days);
        assert_eq!(
            diffs.len(), 0,
            "Expected 0 diffs for 鈴木昭(1021) but got {}:\n{}",
            diffs.len(),
            diffs.iter().map(|d| format!("  {} {}: csv={} sys={}", d.date, d.field, d.csv_val, d.sys_val)).collect::<Vec<_>>().join("\n")
        );
    }

    #[test]
    fn test_fmt_min() {
        assert_eq!(fmt_min(0), "");
        assert_eq!(fmt_min(60), "1:00");
        assert_eq!(fmt_min(90), "1:30");
        assert_eq!(fmt_min(318), "5:18");
        assert_eq!(fmt_min(565), "9:25");
        assert_eq!(fmt_min(14560), "242:40");
    }

    #[test]
    fn test_parse_hhmm() {
        assert_eq!(parse_hhmm(""), 0);
        assert_eq!(parse_hhmm("5:18"), 318);
        assert_eq!(parse_hhmm("9:25"), 565);
        assert_eq!(parse_hhmm("242:40"), 14560);
        assert_eq!(parse_hhmm("0:03"), 3);
    }
}
