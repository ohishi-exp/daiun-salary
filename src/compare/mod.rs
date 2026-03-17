//! 拘束時間管理表 CSV 比較ライブラリ
//!
//! compare.rs CLI と restraint_report.rs API の共通ロジック

use std::collections::{BTreeMap, HashMap};

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use serde::Serialize;

use crate::csv_parser;
use crate::csv_parser::kudgivt::KudgivtRow;
use crate::csv_parser::kudguri::KudguriRow;
use crate::csv_parser::work_segments::{self, calc_late_night_mins, EventClass, Workday};

// ========== 共通型 ==========

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
pub struct DiffItem {
    pub date: String,
    pub field: String,
    pub csv_val: String,
    pub sys_val: String,
}

#[derive(Debug, Serialize)]
pub struct CompareReport {
    pub drivers: Vec<DriverCompareResult>,
    pub total_diffs: usize,
}

#[derive(Debug, Serialize)]
pub struct DriverCompareResult {
    pub driver_name: String,
    pub driver_cd: String,
    pub diffs: Vec<DiffItem>,
    pub total_diffs: Vec<TotalDiffItem>,
}

#[derive(Debug, Serialize)]
pub struct TotalDiffItem {
    pub label: String,
    pub csv_val: String,
    pub sys_val: String,
}

// ========== ユーティリティ ==========

pub fn fmt_min(val: i32) -> String {
    if val == 0 {
        return String::new();
    }
    format!("{}:{:02}", val / 60, val.abs() % 60)
}

fn normalize_time(s: &str) -> String {
    let s = s.trim();
    if s.is_empty() {
        return String::new();
    }
    if let Some((h, m)) = s.split_once(':') {
        let h_num: u32 = h.parse().unwrap_or(0);
        format!("{}:{}", h_num, m)
    } else {
        s.to_string()
    }
}

/// 秒を切り捨てて分精度に揃える
pub fn trunc_min(dt: NaiveDateTime) -> NaiveDateTime {
    dt.with_second(0).unwrap_or(dt)
}

// ========== CSV パース ==========

pub fn parse_restraint_csv(bytes: &[u8]) -> Result<Vec<CsvDriverData>, String> {
    let text = if let Ok(s) = String::from_utf8(bytes.to_vec()) {
        s
    } else {
        let (decoded, _, _) = encoding_rs::SHIFT_JIS.decode(bytes);
        decoded.into_owned()
    };

    let mut drivers = Vec::new();
    let mut current: Option<CsvDriverData> = None;
    let mut in_data = false;

    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }

        if line.starts_with("氏名,") {
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

        if line.starts_with("日付,") {
            in_data = true;
            continue;
        }

        let Some(ref mut driver) = current else {
            continue;
        };
        if !in_data {
            continue;
        }

        let cols: Vec<&str> = line.split(',').collect();

        if cols.first().map(|s| s.contains("合計")).unwrap_or(false) {
            driver.total_drive = cols.get(3).unwrap_or(&"").to_string();
            driver.total_cargo = cols.get(5).unwrap_or(&"").to_string();
            driver.total_break = cols.get(7).unwrap_or(&"").to_string();
            driver.total_restraint = cols.get(11).unwrap_or(&"").to_string();
            driver.total_actual_work = cols.get(18).unwrap_or(&"").to_string();
            driver.total_overtime = cols.get(19).unwrap_or(&"").to_string();
            driver.total_late_night = cols.get(20).unwrap_or(&"").to_string();
            driver.total_ot_late_night = cols.get(21).unwrap_or(&"").to_string();
            in_data = false;
            continue;
        }

        let date_str = cols.first().unwrap_or(&"").to_string();
        if !date_str.contains('月') {
            continue;
        }

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

    if drivers.is_empty() {
        return Err("ドライバーが見つかりません".to_string());
    }

    Ok(drivers)
}

// ========== 差分検出 ==========

/// CsvDayRow同士の差分を検出（日付ベースマッチング）
pub fn detect_diffs_csv(csv_days: &[CsvDayRow], sys_days: &[CsvDayRow]) -> Vec<DiffItem> {
    let mut diffs = Vec::new();

    let mut sys_idx = 0;
    for csv_day in csv_days {
        if csv_day.is_holiday {
            continue;
        }

        let sys_day = sys_days[sys_idx..]
            .iter()
            .find(|s| s.date == csv_day.date && !s.is_holiday);
        let sys_day = match sys_day {
            Some(sd) => {
                if let Some(pos) = sys_days[sys_idx..].iter().position(|s| std::ptr::eq(s, sd)) {
                    sys_idx += pos + 1;
                }
                sd
            }
            None => continue,
        };

        let csv_start = normalize_time(&csv_day.start_time);
        let sys_start = normalize_time(&sys_day.start_time);
        let csv_end = normalize_time(&csv_day.end_time);
        let sys_end = normalize_time(&sys_day.end_time);
        let checks = [
            ("始業", &csv_start, &sys_start),
            ("終業", &csv_end, &sys_end),
            ("運転", &csv_day.drive, &sys_day.drive),
            ("重複運転", &csv_day.overlap_drive, &sys_day.overlap_drive),
            ("小計", &csv_day.subtotal, &sys_day.subtotal),
            (
                "重複小計",
                &csv_day.overlap_subtotal,
                &sys_day.overlap_subtotal,
            ),
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

/// 参照CSVの日付データから対象年月を推定
pub fn detect_year_month(drivers: &[CsvDriverData]) -> (i32, u32) {
    for d in drivers {
        for day in &d.days {
            if day.is_holiday {
                continue;
            }
            if let Some(m_pos) = day.date.find('月') {
                if let Ok(m) = day.date[..m_pos].parse::<u32>() {
                    // 年はCSVヘッダーから取れないので2026固定（要改善）
                    return (2026, m);
                }
            }
        }
    }
    (2026, 1)
}

// ========== ドライバー比較 ==========

pub fn compare_drivers(
    drivers1: &[CsvDriverData],
    drivers2: &[CsvDriverData],
    driver_filter: Option<&str>,
) -> CompareReport {
    let mut report = CompareReport {
        drivers: Vec::new(),
        total_diffs: 0,
    };

    for d1 in drivers1 {
        if let Some(f) = driver_filter {
            if d1.driver_cd != f {
                continue;
            }
        }
        let d2 = drivers2.iter().find(|d| d.driver_cd == d1.driver_cd);
        let Some(d2) = d2 else {
            report.drivers.push(DriverCompareResult {
                driver_name: d1.driver_name.clone(),
                driver_cd: d1.driver_cd.clone(),
                diffs: Vec::new(),
                total_diffs: vec![TotalDiffItem {
                    label: "エラー".to_string(),
                    csv_val: "存在".to_string(),
                    sys_val: "該当なし".to_string(),
                }],
            });
            continue;
        };

        let diffs = detect_diffs_csv(&d1.days, &d2.days);

        let mut total_diffs = Vec::new();
        let total_checks = [
            ("運転合計", &d1.total_drive, &d2.total_drive),
            ("拘束合計", &d1.total_restraint, &d2.total_restraint),
            ("実働合計", &d1.total_actual_work, &d2.total_actual_work),
            ("時間外合計", &d1.total_overtime, &d2.total_overtime),
            ("深夜合計", &d1.total_late_night, &d2.total_late_night),
        ];
        for (label, v1, v2) in total_checks {
            let a = v1.trim();
            let b = v2.trim();
            if a != b && !(a.is_empty() && b.is_empty()) {
                total_diffs.push(TotalDiffItem {
                    label: label.to_string(),
                    csv_val: a.to_string(),
                    sys_val: b.to_string(),
                });
            }
        }

        let diff_count = diffs.len() + total_diffs.len();
        report.total_diffs += diff_count;

        report.drivers.push(DriverCompareResult {
            driver_name: d1.driver_name.clone(),
            driver_cd: d1.driver_cd.clone(),
            diffs,
            total_diffs,
        });
    }

    report
}

// ========== ZIP → インメモリ計算 ==========

fn default_classifications() -> HashMap<String, EventClass> {
    let mut m = HashMap::new();
    m.insert("201".to_string(), EventClass::Drive);
    m.insert("202".to_string(), EventClass::Cargo);
    m.insert("203".to_string(), EventClass::Cargo);
    m.insert("204".to_string(), EventClass::Cargo); // その他 → 荷役
    m.insert("302".to_string(), EventClass::RestSplit);
    m.insert("301".to_string(), EventClass::Break);
    m
}

/// 実働ベースの時間外深夜計算
/// Drive/Cargoイベントの累計が480分に達した後の深夜時間を返す
pub fn calc_ot_late_night_from_events(events: &[(NaiveDateTime, NaiveDateTime)]) -> i32 {
    let mut cumulative = 0i64;
    let mut ot_night = 0i32;
    for &(start, end) in events {
        let dur = (end - start).num_minutes();
        if dur <= 0 {
            continue;
        }
        if cumulative >= 480 {
            // 全て時間外
            ot_night += calc_late_night_mins(start, end);
        } else if cumulative + dur <= 480 {
            // 全て所定内
        } else {
            // 境界を跨ぐ: 480分到達点で分割
            let regular_dur = 480 - cumulative;
            let boundary = start + chrono::Duration::minutes(regular_dur);
            ot_night += calc_late_night_mins(boundary, end);
        }
        cumulative += dur;
    }
    ot_night
}

pub fn group_operations_into_work_days(rows: &[KudguriRow]) -> HashMap<String, NaiveDate> {
    const REST_THRESHOLD_MINUTES: i64 = 540;
    const MAX_WORK_DAY_MINUTES: i64 = 1440;

    let mut unko_work_date: HashMap<String, NaiveDate> = HashMap::new();
    let mut driver_rows: HashMap<String, Vec<&KudguriRow>> = HashMap::new();
    for row in rows {
        if !row.driver_cd.is_empty() {
            driver_rows
                .entry(row.driver_cd.clone())
                .or_default()
                .push(row);
        }
    }

    for (_driver_cd, mut ops) in driver_rows {
        ops.sort_by(|a, b| {
            let da = a.departure_at.or(a.garage_out_at);
            let db = b.departure_at.or(b.garage_out_at);
            da.cmp(&db)
        });

        let mut current_shigyo: Option<NaiveDateTime> = None;
        let mut current_work_date: Option<NaiveDate> = None;
        let mut last_end: Option<NaiveDateTime> = None;

        for row in &ops {
            let dep = match row.departure_at.or(row.garage_out_at) {
                Some(d) => d,
                None => {
                    let wd = row.operation_date.unwrap_or(row.reading_date);
                    unko_work_date.insert(row.unko_no.clone(), wd);
                    continue;
                }
            };
            let ret = row.return_at.or(row.garage_in_at).unwrap_or(dep);

            let new_day = if let (Some(shigyo), Some(prev_end)) = (current_shigyo, last_end) {
                let gap_minutes = (dep - prev_end).num_minutes();
                let since_shigyo_minutes = (dep - shigyo).num_minutes();
                gap_minutes >= REST_THRESHOLD_MINUTES
                    || since_shigyo_minutes >= MAX_WORK_DAY_MINUTES
            } else {
                true
            };

            if new_day {
                current_shigyo = Some(dep);
                current_work_date = Some(dep.date());
            }

            unko_work_date.insert(row.unko_no.clone(), current_work_date.unwrap());
            last_end = Some(match last_end {
                Some(prev) if ret > prev => ret,
                Some(prev) => prev,
                None => ret,
            });
        }
    }

    unko_work_date
}

fn parse_ferry_minutes(zip_files: &[(String, Vec<u8>)]) -> HashMap<String, i32> {
    let mut ferry_map = HashMap::new();
    for (name, bytes) in zip_files {
        if !name.to_uppercase().contains("KUDGFRY") {
            continue;
        }
        let text = csv_parser::decode_shift_jis(bytes);
        for line in text.lines().skip(1) {
            let cols: Vec<&str> = line.split(',').collect();
            if cols.len() <= 11 {
                continue;
            }
            let unko_no = cols[0].trim().to_string();
            if let (Some(start), Some(end)) = (
                NaiveDateTime::parse_from_str(cols[10].trim(), "%Y/%m/%d %H:%M:%S")
                    .ok()
                    .or_else(|| {
                        NaiveDateTime::parse_from_str(cols[10].trim(), "%Y/%m/%d %k:%M:%S").ok()
                    }),
                NaiveDateTime::parse_from_str(cols[11].trim(), "%Y/%m/%d %H:%M:%S")
                    .ok()
                    .or_else(|| {
                        NaiveDateTime::parse_from_str(cols[11].trim(), "%Y/%m/%d %k:%M:%S").ok()
                    }),
            ) {
                let secs = (end - start).num_seconds();
                let mins = ((secs + 30) / 60) as i32;
                if mins > 0 {
                    *ferry_map.entry(unko_no).or_insert(0) += mins;
                }
            }
        }
    }
    ferry_map
}

pub fn split_work_segments_at_boundary(
    segments: Vec<work_segments::WorkSegment>,
    boundary: NaiveDateTime,
) -> Vec<work_segments::WorkSegment> {
    let mut result = Vec::new();
    for seg in segments {
        if seg.start < boundary && seg.end > boundary {
            let total_mins = (seg.end - seg.start).num_minutes().max(1) as f64;
            let before_mins = (boundary - seg.start).num_minutes() as f64;
            let ratio = before_mins / total_mins;
            let d1 = (seg.drive_minutes as f64 * ratio).round() as i32;
            let c1 = (seg.cargo_minutes as f64 * ratio).round() as i32;
            let l1 = (seg.labor_minutes as f64 * ratio).round() as i32;
            result.push(work_segments::WorkSegment {
                start: seg.start,
                end: boundary,
                labor_minutes: l1,
                drive_minutes: d1,
                cargo_minutes: c1,
            });
            result.push(work_segments::WorkSegment {
                start: boundary,
                end: seg.end,
                labor_minutes: seg.labor_minutes - l1,
                drive_minutes: seg.drive_minutes - d1,
                cargo_minutes: seg.cargo_minutes - c1,
            });
        } else {
            result.push(seg);
        }
    }
    result
}

/// フェリー控除用の事前計算データ（compare/upload共通）
#[derive(Clone, Default)]
pub struct FerryInfo {
    /// unko_no → フェリー時間（分、四捨五入）
    pub ferry_minutes: HashMap<String, i32>,
    /// unko_no → 対応する301(休憩)イベントのduration合計
    pub ferry_break_dur: HashMap<String, i32>,
    /// unko_no → フェリー乗船期間(start, end)リスト
    pub ferry_period_map: HashMap<String, Vec<(NaiveDateTime, NaiveDateTime)>>,
}

impl FerryInfo {
    /// zip_files からフェリー情報を構築
    pub fn from_zip_files(
        zip_files: &[(String, Vec<u8>)],
        kudgivt_by_unko: &HashMap<String, Vec<&KudgivtRow>>,
    ) -> Self {
        let ferry_minutes = parse_ferry_minutes(zip_files);

        let mut ferry_break_dur: HashMap<String, i32> = HashMap::new();
        let mut ferry_period_map: HashMap<String, Vec<(NaiveDateTime, NaiveDateTime)>> =
            HashMap::new();
        for (name, bytes) in zip_files {
            if !name.to_uppercase().contains("KUDGFRY") {
                continue;
            }
            let text = csv_parser::decode_shift_jis(bytes);
            for line in text.lines().skip(1) {
                let cols: Vec<&str> = line.split(',').collect();
                if cols.len() <= 11 {
                    continue;
                }
                let unko_no = cols[0].trim().to_string();
                if let (Some(s), Some(e)) = (
                    NaiveDateTime::parse_from_str(cols[10].trim(), "%Y/%m/%d %H:%M:%S")
                        .ok()
                        .or_else(|| {
                            NaiveDateTime::parse_from_str(cols[10].trim(), "%Y/%m/%d %k:%M:%S").ok()
                        }),
                    NaiveDateTime::parse_from_str(cols[11].trim(), "%Y/%m/%d %H:%M:%S")
                        .ok()
                        .or_else(|| {
                            NaiveDateTime::parse_from_str(cols[11].trim(), "%Y/%m/%d %k:%M:%S").ok()
                        }),
                ) {
                    ferry_period_map
                        .entry(unko_no.clone())
                        .or_default()
                        .push((s, e));
                    // 対応する301イベントをマッチ
                    if let Some(events) = kudgivt_by_unko.get(&unko_no) {
                        let matching_301 = events
                            .iter()
                            .filter(|ev| {
                                ev.event_cd == "301" && ev.duration_minutes.unwrap_or(0) > 0
                            })
                            .min_by_key(|ev| (ev.start_at - s).num_seconds().abs());
                        if let Some(evt) = matching_301 {
                            let dur = evt.duration_minutes.unwrap_or(0);
                            *ferry_break_dur.entry(unko_no).or_insert(0) += dur;
                        }
                    }
                }
            }
        }

        FerryInfo {
            ferry_minutes,
            ferry_break_dur,
            ferry_period_map,
        }
    }
}

/// ZIP を処理して CsvDriverData を生成
/// 日別集計データ（compare/upload共通）
#[derive(Clone, Default)]
pub struct DayAgg {
    pub total_work_minutes: i32,
    pub late_night_minutes: i32,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
    pub unko_nos: Vec<String>,
    pub segments: Vec<SegRec>,
    pub overlap_drive_minutes: i32,
    pub overlap_cargo_minutes: i32,
    pub overlap_break_minutes: i32,
    pub overlap_restraint_minutes: i32,
    pub ot_late_night_minutes: i32,
    pub from_multi_op: bool,
}

#[derive(Clone)]
pub struct SegRec {
    pub start_at: NaiveDateTime,
    pub end_at: NaiveDateTime,
}

pub fn post_process_day_map(
    day_map: &mut HashMap<(String, NaiveDate, NaiveTime), DayAgg>,
    workday_boundaries: &mut HashMap<
        (String, NaiveDate, NaiveTime),
        (NaiveDateTime, NaiveDateTime),
    >,
    day_work_events: &mut HashMap<
        (String, NaiveDate, NaiveTime),
        Vec<(NaiveDateTime, NaiveDateTime)>,
    >,
    kudgivt_by_unko: &HashMap<String, Vec<&KudgivtRow>>,
    classifications: &HashMap<String, EventClass>,
    kudguri_rows: &[KudguriRow],
    ferry_info: &FerryInfo,
) {
    // ---- 構内結合 ----
    {
        let keys: Vec<_> = day_map.keys().cloned().collect();
        let mut driver_date_keys: HashMap<
            (String, NaiveDate),
            Vec<(String, NaiveDate, NaiveTime)>,
        > = HashMap::new();
        for (dc, d, st) in &keys {
            driver_date_keys
                .entry((dc.clone(), *d))
                .or_default()
                .push((dc.clone(), *d, *st));
        }

        for ((_dc, _d), mut entries) in driver_date_keys {
            if entries.len() < 2 {
                continue;
            }
            entries.sort_by_key(|(_, _, st)| *st);

            let mut merged_any = true;
            while merged_any {
                merged_any = false;
                for i in 0..entries.len().saturating_sub(1) {
                    let key_a = entries[i].clone();
                    let key_b = entries[i + 1].clone();
                    let merge_info = {
                        let agg_a = match day_map.get(&key_a) {
                            Some(a) => a,
                            None => continue,
                        };
                        let agg_b = match day_map.get(&key_b) {
                            Some(b) => b,
                            None => continue,
                        };
                        let different_ops =
                            !agg_a.unko_nos.iter().any(|u| agg_b.unko_nos.contains(u));
                        let gap_info = match (
                            agg_a.segments.iter().map(|s| s.end_at).max(),
                            agg_b.segments.iter().map(|s| s.start_at).min(),
                        ) {
                            (Some(pe), Some(ns)) => {
                                let gap_secs = (ns - pe).num_seconds();
                                let gap = ((gap_secs + 30) / 60) as i64; // 四捨五入
                                if gap >= 0 && gap < 180 {
                                    Some(gap as i32)
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        };
                        if different_ops {
                            gap_info
                        } else {
                            None
                        }
                    };
                    if let Some(gap_mins) = merge_info {
                        let b_clone = day_map.get(&key_b).unwrap().clone();
                        let agg_a_mut = day_map.get_mut(&key_a).unwrap();
                        agg_a_mut.drive_minutes += b_clone.drive_minutes;
                        agg_a_mut.cargo_minutes += b_clone.cargo_minutes;
                        agg_a_mut.total_work_minutes += b_clone.total_work_minutes + gap_mins;
                        agg_a_mut.late_night_minutes += b_clone.late_night_minutes;
                        agg_a_mut.ot_late_night_minutes += b_clone.ot_late_night_minutes;
                        agg_a_mut.segments.extend(b_clone.segments);
                        for u in &b_clone.unko_nos {
                            if !agg_a_mut.unko_nos.contains(u) {
                                agg_a_mut.unko_nos.push(u.clone());
                            }
                        }
                        // day_work_eventsも結合
                        if let Some(b_events) =
                            day_work_events.remove(&(key_b.0.clone(), key_b.1, key_b.2))
                        {
                            day_work_events
                                .entry((key_a.0.clone(), key_a.1, key_a.2))
                                .or_default()
                                .extend(b_events);
                        }
                        day_map.remove(&key_b);
                        entries.remove(i + 1);
                        merged_any = true;
                        break;
                    }
                }
            }
        }
    }

    // ---- overlap計算 ----
    {
        struct DayInfo {
            start: NaiveDateTime,
            end: NaiveDateTime,
            unko_nos: Vec<String>,
        }

        let mut driver_days: HashMap<String, BTreeMap<(NaiveDate, NaiveTime), DayInfo>> =
            HashMap::new();
        for ((driver_cd, date, st), agg) in day_map.iter() {
            if agg.segments.is_empty() {
                continue;
            }
            let start = trunc_min(agg.segments.iter().map(|s| s.start_at).min().unwrap());
            let end = trunc_min(agg.segments.iter().map(|s| s.end_at).max().unwrap());
            driver_days.entry(driver_cd.clone()).or_default().insert(
                (*date, *st),
                DayInfo {
                    start,
                    end,
                    unko_nos: agg.unko_nos.clone(),
                },
            );
        }

        for (driver_cd, dates_map) in &driver_days {
            let dates: Vec<(NaiveDate, NaiveTime)> = dates_map.keys().copied().collect();
            let mut effective_start: Option<NaiveDateTime> = None;
            let mut prev_end: Option<NaiveDateTime> = None;
            let mut next_day_deduction: Option<(i32, i32, i32, i32)> = None;
            let mut split_rests: Vec<i32> = Vec::new();

            for (idx, &(date, st)) in dates.iter().enumerate() {
                let info = &dates_map[&(date, st)];

                // deductionは先に適用（resetで消される前に）
                if let Some((ded_drive, ded_cargo, ded_restraint, ded_night)) =
                    next_day_deduction.take()
                {
                    if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), date, st)) {
                        agg.drive_minutes = (agg.drive_minutes - ded_drive).max(0);
                        agg.cargo_minutes = (agg.cargo_minutes - ded_cargo).max(0);
                        agg.total_work_minutes = (agg.total_work_minutes - ded_restraint).max(0);
                        agg.late_night_minutes = (agg.late_night_minutes - ded_night).max(0);
                    }
                }

                let reset = match prev_end {
                    Some(pe) => (info.start - pe).num_minutes() >= 480,
                    None => true,
                };
                if reset {
                    let key = (driver_cd.clone(), date, st);
                    if let Some(&(wb_start, _)) = workday_boundaries.get(&key) {
                        // merge由来の24h境界がある → chain最終日
                        // effective_startは24h境界から（window計算に使用）
                        effective_start = Some(wb_start);
                        // endは実際のsegment endに更新
                        let seg_end = day_map
                            .get(&key)
                            .and_then(|a| a.segments.iter().map(|s| s.end_at).max())
                            .unwrap_or(info.end);
                        workday_boundaries.insert(key, (wb_start, seg_end));
                    } else {
                        effective_start = Some(info.start);
                    }
                } else {
                    effective_start = Some(effective_start.unwrap() + chrono::Duration::hours(24));
                }

                let window_end = effective_start.unwrap() + chrono::Duration::hours(24);

                if idx + 1 < dates.len() {
                    let (next_date, next_st) = dates[idx + 1];
                    let next_info = &dates_map[&(next_date, next_st)];

                    let mut ol_drive = 0i32;
                    let mut ol_cargo = 0i32;
                    let mut ol_restraint = 0i32;
                    let mut ol_late_night_dc = 0i32; // Drive/Cargoのみの深夜時間
                    let mut ol_work_events: Vec<(NaiveDateTime, NaiveDateTime)> = Vec::new();

                    for unko_no in &next_info.unko_nos {
                        if let Some(events) = kudgivt_by_unko.get(unko_no) {
                            for evt in events {
                                let cls = classifications.get(&evt.event_cd);
                                let dur = evt.duration_minutes.unwrap_or(0);
                                if dur <= 0 {
                                    continue;
                                }
                                let evt_start = trunc_min(evt.start_at);
                                if evt_start >= window_end {
                                    continue;
                                }
                                let evt_end = evt_start + chrono::Duration::minutes(dur as i64);
                                if evt_end <= info.end {
                                    continue;
                                }
                                if evt_start < info.end {
                                    continue;
                                }
                                let overlap_start = evt_start.max(next_info.start);
                                let effective_end = evt_end.min(window_end);
                                if effective_end <= overlap_start {
                                    continue;
                                }
                                let mins = (effective_end - overlap_start).num_minutes() as i32;
                                if mins <= 0 {
                                    continue;
                                }
                                let actual_dur = if mins >= dur { dur } else { mins };
                                match cls {
                                    Some(EventClass::Drive) => {
                                        ol_drive += actual_dur;
                                        ol_late_night_dc +=
                                            calc_late_night_mins(overlap_start, effective_end);
                                        ol_work_events.push((overlap_start, effective_end));
                                    }
                                    Some(EventClass::Cargo) => {
                                        ol_cargo += actual_dur;
                                        ol_late_night_dc +=
                                            calc_late_night_mins(overlap_start, effective_end);
                                        ol_work_events.push((overlap_start, effective_end));
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    if next_info.start < window_end {
                        let restraint_end = day_map
                            .get(&(driver_cd.clone(), next_date, next_st))
                            .map(|next_agg| {
                                next_agg
                                    .segments
                                    .iter()
                                    .filter(|s| trunc_min(s.start_at) < window_end)
                                    .map(|s| trunc_min(s.end_at).min(window_end))
                                    .max()
                                    .unwrap_or(window_end)
                            })
                            .unwrap_or(window_end);
                        if restraint_end > next_info.start {
                            ol_restraint = (restraint_end - next_info.start).num_minutes() as i32;
                        }
                    }

                    let next_gap = (next_info.start - info.end).num_minutes();
                    // 長距離判定: 現在or次のエントリの運行が日跨ぎ（宿泊伴う）なら480（例外）、
                    // それ以外は540（原則）
                    let is_long_distance = {
                        let check_unko = |unko_nos: &[String]| -> bool {
                            unko_nos.iter().any(|u| {
                                kudguri_rows.iter().any(|r| {
                                    r.unko_no == *u
                                        && r.departure_at
                                            .zip(r.return_at)
                                            .map(|(dep, ret)| dep.date() != ret.date())
                                            .unwrap_or(false)
                                })
                            })
                        };
                        check_unko(&info.unko_nos) || check_unko(&next_info.unko_nos)
                    };
                    let rest_threshold = if is_long_distance { 480 } else { 540 };
                    let mut next_resets = next_gap >= rest_threshold;
                    // 分割特例: 180分以上の休息を蓄積して判定
                    if !next_resets && next_gap >= 180 {
                        split_rests.push(next_gap as i32);
                        let total: i32 = split_rests.iter().sum();
                        let threshold = match split_rests.len() {
                            2 => 600,
                            n if n >= 3 => 720,
                            _ => i32::MAX,
                        };
                        if total >= threshold {
                            next_resets = true;
                            split_rests.clear();
                        }
                    } else if next_resets {
                        split_rests.clear();
                    }

                    // 同日かつ長めのgap(≥180分)は重複表示（24h境界の分割）
                    let same_date_long_gap = date == next_date && next_gap >= 180;
                    if !next_resets && ol_restraint > 0 && !same_date_long_gap {
                        if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), date, st)) {
                            agg.drive_minutes += ol_drive;
                            agg.cargo_minutes += ol_cargo;
                            agg.total_work_minutes += ol_restraint;
                            agg.late_night_minutes += ol_late_night_dc;
                        }
                        // overlapのDrive/Cargoイベントをday_work_eventsに追加してot_late_night再計算
                        if !ol_work_events.is_empty() {
                            let events_entry = day_work_events
                                .entry((driver_cd.clone(), date, st))
                                .or_default();
                            events_entry.extend(ol_work_events);
                            let mut sorted = events_entry.clone();
                            sorted.sort_by_key(|&(s, _)| s);
                            let ot_night = calc_ot_late_night_from_events(&sorted);
                            if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), date, st)) {
                                agg.ot_late_night_minutes = ot_night;
                            }
                        }
                        next_day_deduction =
                            Some((ol_drive, ol_cargo, ol_restraint, ol_late_night_dc));
                        // 24h境界表示: merge時にworkday_boundariesを更新
                        let eff_start = effective_start.unwrap();
                        // 現在エントリ: start=effective_start, end=window_end(24h境界)
                        workday_boundaries
                            .insert((driver_cd.clone(), date, st), (eff_start, window_end));
                        // 次エントリ: start=window_end(24h境界), end=実際のsegment終了
                        // (chain最終日はendが実際の時刻で表示される)
                        let next_seg_end = day_map
                            .get(&(driver_cd.clone(), next_date, next_st))
                            .and_then(|a| a.segments.iter().map(|s| s.end_at).max())
                            .unwrap_or(window_end + chrono::Duration::hours(24));
                        workday_boundaries.insert(
                            (driver_cd.clone(), next_date, next_st),
                            (window_end, next_seg_end),
                        );
                    } else if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), date, st)) {
                        agg.overlap_drive_minutes = ol_drive;
                        agg.overlap_cargo_minutes = ol_cargo;
                        agg.overlap_break_minutes = (ol_restraint - ol_drive - ol_cargo).max(0);
                        agg.overlap_restraint_minutes = ol_restraint;
                    }
                }

                prev_end = Some(info.end);
            }
        }
    }

    // ---- フェリー控除 ----
    for ((_driver_cd, _date, _st), agg) in day_map.iter_mut() {
        let mut ferry_deduction = 0i32; // KUDGFRY時間（四捨五入）
        let mut ferry_break_deduction = 0i32; // 対応301イベントのduration
        let mut ferry_drive_overlap = 0i32; // Drive/Cargoとフェリー期間の実重複
        for unko in &agg.unko_nos {
            if let Some(&fm) = ferry_info.ferry_minutes.get(unko) {
                ferry_deduction += fm;
            }
            if let Some(&fb) = ferry_info.ferry_break_dur.get(unko) {
                ferry_break_deduction += fb;
            }
            if let Some(periods) = ferry_info.ferry_period_map.get(unko) {
                if let Some(events) = kudgivt_by_unko.get(unko) {
                    for &(fs, fe) in periods {
                        for evt in events {
                            match classifications.get(&evt.event_cd) {
                                Some(EventClass::Drive) | Some(EventClass::Cargo) => {}
                                _ => continue,
                            }
                            let dur = evt.duration_minutes.unwrap_or(0);
                            if dur <= 0 {
                                continue;
                            }
                            // フェリー重複判定は秒精度（trunc_minしない）
                            let es = evt.start_at;
                            let ee = es + chrono::Duration::minutes(dur as i64);
                            let os = es.max(fs);
                            let oe = ee.min(fe);
                            if oe > os {
                                let secs = (oe - os).num_seconds();
                                ferry_drive_overlap += ((secs + 30) / 60) as i32;
                            }
                        }
                    }
                }
            }
        }
        if ferry_deduction > 0 {
            // drive控除 = 丸め差(ferry-break)と実重複の小さい方
            let rounding_diff = (ferry_deduction - ferry_break_deduction).max(0);
            let drive_ded = rounding_diff.min(ferry_drive_overlap);
            // total_work控除 = break + drive控除分
            let total_ded = ferry_break_deduction + drive_ded;
            agg.total_work_minutes = (agg.total_work_minutes - total_ded).max(0);
            agg.drive_minutes = (agg.drive_minutes - drive_ded).max(0);
        }
    }
}

pub fn process_zip(
    zip_bytes: &[u8],
    target_year: i32,
    target_month: u32,
) -> Result<Vec<CsvDriverData>, String> {
    let zip_files =
        csv_parser::extract_zip(zip_bytes).map_err(|e| format!("ZIP展開エラー: {e}"))?;

    let kudguri_bytes = zip_files
        .iter()
        .find(|(n, _)| n.to_uppercase().contains("KUDGURI"))
        .ok_or("KUDGURI.csv が見つかりません")?;
    let kudgivt_bytes = zip_files
        .iter()
        .find(|(n, _)| n.to_uppercase().contains("KUDGIVT"))
        .ok_or("KUDGIVT.csv が見つかりません")?;

    let kudguri_text = csv_parser::decode_shift_jis(&kudguri_bytes.1);
    let kudgivt_text = csv_parser::decode_shift_jis(&kudgivt_bytes.1);

    let kudguri_rows = csv_parser::kudguri::parse_kudguri(&kudguri_text)
        .map_err(|e| format!("KUDGURIパースエラー: {e}"))?;
    let kudgivt_rows = csv_parser::kudgivt::parse_kudgivt(&kudgivt_text)
        .map_err(|e| format!("KUDGIVTパースエラー: {e}"))?;

    let classifications = default_classifications();
    let unko_work_date = group_operations_into_work_days(&kudguri_rows);

    let mut kudgivt_by_unko: HashMap<String, Vec<&KudgivtRow>> = HashMap::new();
    for row in &kudgivt_rows {
        kudgivt_by_unko
            .entry(row.unko_no.clone())
            .or_default()
            .push(row);
    }

    // DayAgg, SegRec はモジュールレベルで定義（pub）

    let mut workday_boundaries: HashMap<
        (String, NaiveDate, NaiveTime),
        (NaiveDateTime, NaiveDateTime),
    > = HashMap::new();
    let mut day_map: HashMap<(String, NaiveDate, NaiveTime), DayAgg> = HashMap::new();
    let mut unko_segments: HashMap<
        String,
        Vec<(NaiveDateTime, NaiveDateTime, NaiveDate, NaiveTime)>,
    > = HashMap::new();
    let mut multi_op_boundaries: HashMap<String, NaiveDateTime> = HashMap::new();

    let mut workday_groups: BTreeMap<(String, NaiveDate), Vec<&KudguriRow>> = BTreeMap::new();
    for row in &kudguri_rows {
        let wd = unko_work_date
            .get(&row.unko_no)
            .copied()
            .unwrap_or(row.operation_date.unwrap_or(row.reading_date));
        workday_groups
            .entry((row.driver_cd.clone(), wd))
            .or_default()
            .push(row);
    }

    for ((_group_driver_cd, _group_work_date), ops) in &workday_groups {
        let valid_ops: Vec<&&KudguriRow> = ops
            .iter()
            .filter(|r| matches!((r.departure_at, r.return_at), (Some(d), Some(r)) if r > d))
            .collect();

        for row in ops {
            if matches!((row.departure_at, row.return_at), (Some(d), Some(r)) if r > d) {
                continue;
            }
            let work_date = row.operation_date.unwrap_or(row.reading_date);
            let total_drive_mins = row.drive_time_general.unwrap_or(0)
                + row.drive_time_highway.unwrap_or(0)
                + row.drive_time_bypass.unwrap_or(0);
            let default_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
            let entry = day_map
                .entry((row.driver_cd.clone(), work_date, default_time))
                .or_insert(DayAgg {
                    total_work_minutes: 0,
                    late_night_minutes: 0,
                    drive_minutes: 0,
                    cargo_minutes: 0,
                    unko_nos: Vec::new(),
                    segments: Vec::new(),
                    overlap_drive_minutes: 0,
                    overlap_cargo_minutes: 0,
                    overlap_break_minutes: 0,
                    overlap_restraint_minutes: 0,
                    ot_late_night_minutes: 0,
                    from_multi_op: false,
                });
            entry.total_work_minutes += total_drive_mins;
            entry.unko_nos.push(row.unko_no.clone());
        }

        if valid_ops.is_empty() {
            continue;
        }

        let spans_different_days = if valid_ops.len() > 1 {
            let dates: std::collections::HashSet<NaiveDate> = valid_ops
                .iter()
                .filter_map(|r| r.departure_at.map(|d| d.date()))
                .collect();
            if dates.len() <= 1 {
                false
            } else {
                // 前の運行の帰着日と次の運行の出発日が同じ場合は
                // multi-op mergeを使わない（overlapセクションに任せる）
                let mut sorted_valid: Vec<_> = valid_ops.iter().collect();
                sorted_valid.sort_by_key(|r| r.departure_at);
                let ops_share_date = sorted_valid.windows(2).any(|pair| {
                    let ret_date = pair[0].return_at.map(|r| r.date());
                    let dep_date = pair[1].departure_at.map(|d| d.date());
                    ret_date.is_some() && ret_date == dep_date
                });
                !ops_share_date
            }
        } else {
            false
        };

        if !spans_different_days {
            for row in &valid_ops {
                let dep = row.departure_at.unwrap();
                let ret = row.return_at.unwrap();
                let events = kudgivt_by_unko.get(&row.unko_no);
                let event_slice: Vec<&KudgivtRow> = events.map(|e| e.to_vec()).unwrap_or_default();

                let rest_events_for_unko: Vec<(NaiveDateTime, i32)> = event_slice
                    .iter()
                    .filter(|e| classifications.get(&e.event_cd) == Some(&EventClass::RestSplit))
                    .filter_map(|e| {
                        e.duration_minutes
                            .filter(|&d| d > 0)
                            .map(|d| (e.start_at, d))
                    })
                    .collect();
                let workdays = work_segments::determine_workdays(&rest_events_for_unko, dep, ret);

                let segments =
                    work_segments::split_by_rest(dep, ret, &event_slice, &classifications);
                let segments = work_segments::split_segments_at_24h(segments);
                // workday境界でセグメントを分割（長距離運行の24hルール対応）
                // 条件: 3日以上スパン、分割後の両パートが60分以上
                let span_days = (ret.date() - dep.date()).num_days();
                let segments = if span_days >= 3 && workdays.len() >= 2 {
                    let mut segs = segments;
                    for wd in &workdays {
                        if wd.end < ret {
                            let sig_split = segs.iter().any(|seg| {
                                seg.start < wd.end
                                    && wd.end < seg.end
                                    && (wd.end - seg.start).num_minutes() >= 180
                                    && (seg.end - wd.end).num_minutes() >= 180
                            });
                            if sig_split {
                                segs = split_work_segments_at_boundary(segs, wd.end);
                                multi_op_boundaries.insert(row.unko_no.clone(), wd.end);
                            }
                        }
                    }
                    segs
                } else {
                    segments
                };
                let daily_segments = work_segments::split_segments_by_day(&segments);

                for wd in &workdays {
                    workday_boundaries.insert(
                        (row.driver_cd.clone(), wd.date, wd.start.time()),
                        (wd.start, wd.end),
                    );
                }

                let find_start_time = |ts: NaiveDateTime| -> NaiveTime {
                    workdays
                        .iter()
                        .find(|wd| ts >= wd.start && ts < wd.end)
                        .or_else(|| workdays.iter().rev().find(|wd| ts >= wd.start))
                        .map(|wd| wd.start.time())
                        .unwrap_or(dep.time())
                };
                let find_workday_date = |start: NaiveDateTime, end: NaiveDateTime| -> NaiveDate {
                    workdays
                        .iter()
                        .find(|wd| start >= wd.start && end <= wd.end)
                        .map(|wd| wd.date)
                        .unwrap_or(start.date())
                };
                let seg_entries: Vec<_> = segments
                    .iter()
                    .map(|seg| {
                        (
                            seg.start,
                            seg.end,
                            find_workday_date(seg.start, seg.end),
                            find_start_time(seg.start),
                        )
                    })
                    .collect();
                unko_segments.insert(row.unko_no.clone(), seg_entries);

                for ds in &daily_segments {
                    let work_date = workdays
                        .iter()
                        .find(|wd| ds.start >= wd.start && ds.end <= wd.end)
                        .map(|wd| wd.date)
                        .unwrap_or_else(|| {
                            let parent_seg = segments
                                .iter()
                                .find(|seg| ds.start >= seg.start && ds.start < seg.end);
                            parent_seg.map(|seg| seg.start.date()).unwrap_or(ds.date)
                        });
                    let start_time = find_start_time(ds.start);
                    let entry = day_map
                        .entry((row.driver_cd.clone(), work_date, start_time))
                        .or_insert(DayAgg {
                            total_work_minutes: 0,
                            late_night_minutes: 0,
                            drive_minutes: 0,
                            cargo_minutes: 0,
                            unko_nos: Vec::new(),
                            segments: Vec::new(),
                            overlap_drive_minutes: 0,
                            overlap_cargo_minutes: 0,
                            overlap_break_minutes: 0,
                            overlap_restraint_minutes: 0,
                            ot_late_night_minutes: 0,
                            from_multi_op: false,
                        });
                    entry.total_work_minutes += ds.work_minutes;
                    entry.late_night_minutes += ds.late_night_minutes;
                    entry.drive_minutes += ds.drive_minutes;
                    entry.cargo_minutes += ds.cargo_minutes;
                    if !entry.unko_nos.contains(&row.unko_no) {
                        entry.unko_nos.push(row.unko_no.clone());
                    }
                    entry.segments.push(SegRec {
                        start_at: ds.start,
                        end_at: ds.end,
                    });
                }
            }
        } else {
            // ---- 複数運行の結合処理（運行間workday結合） ----
            let merged_dep = valid_ops
                .iter()
                .filter_map(|r| r.departure_at)
                .min()
                .unwrap();
            let merged_ret = valid_ops.iter().filter_map(|r| r.return_at).max().unwrap();

            let merged_dep_trunc = trunc_min(merged_dep);
            // 24h単位でvirtual workdayを生成（3日以上のスパンに対応）
            let mut virtual_workdays: Vec<Workday> = Vec::new();
            let mut boundaries_24h: Vec<NaiveDateTime> = Vec::new();
            let mut boundary = merged_dep_trunc;
            loop {
                let next_boundary = boundary + chrono::Duration::hours(24);
                let wd_start = if virtual_workdays.is_empty() {
                    merged_dep
                } else {
                    boundary
                };
                let wd_end = next_boundary.min(merged_ret);
                virtual_workdays.push(Workday {
                    date: wd_start.date(),
                    start: wd_start,
                    end: wd_end,
                });
                boundaries_24h.push(next_boundary);
                if wd_end >= merged_ret {
                    break;
                }
                boundary = next_boundary;
            }

            for row in &valid_ops {
                // 最初の24h境界をlegacy互換で保存
                multi_op_boundaries.insert(row.unko_no.clone(), boundaries_24h[0]);
            }

            let driver_cd = &valid_ops[0].driver_cd;
            for wd in &virtual_workdays {
                workday_boundaries.insert(
                    (driver_cd.clone(), wd.date, wd.start.time()),
                    (wd.start, wd.end),
                );
            }

            let find_vwd_start_time = |ts: NaiveDateTime| -> NaiveTime {
                virtual_workdays
                    .iter()
                    .find(|wd| ts >= wd.start && ts < wd.end)
                    .or_else(|| virtual_workdays.iter().rev().find(|wd| ts >= wd.start))
                    .map(|wd| wd.start.time())
                    .unwrap_or(merged_dep.time())
            };
            let find_vwd_date = |ts: NaiveDateTime| -> NaiveDate {
                virtual_workdays
                    .iter()
                    .find(|wd| ts >= wd.start && ts < wd.end)
                    .or_else(|| virtual_workdays.iter().rev().find(|wd| ts >= wd.start))
                    .map(|wd| wd.date)
                    .unwrap_or(merged_dep.date())
            };

            for row in &valid_ops {
                let dep = row.departure_at.unwrap();
                let ret = row.return_at.unwrap();
                let events = kudgivt_by_unko.get(&row.unko_no);
                let event_slice: Vec<&KudgivtRow> = events.map(|e| e.to_vec()).unwrap_or_default();

                let segments =
                    work_segments::split_by_rest(dep, ret, &event_slice, &classifications);
                let segments = work_segments::split_segments_at_24h(segments);
                let mut segments = segments;
                for &b in &boundaries_24h {
                    segments = split_work_segments_at_boundary(segments, b);
                }
                let daily_segments = work_segments::split_segments_by_day(&segments);

                let seg_entries: Vec<_> = segments
                    .iter()
                    .map(|seg| {
                        (
                            seg.start,
                            seg.end,
                            find_vwd_date(seg.start),
                            find_vwd_start_time(seg.start),
                        )
                    })
                    .collect();
                unko_segments.insert(row.unko_no.clone(), seg_entries);

                for ds in &daily_segments {
                    let work_date = find_vwd_date(ds.start);
                    let start_time = find_vwd_start_time(ds.start);
                    let entry = day_map
                        .entry((driver_cd.clone(), work_date, start_time))
                        .or_insert(DayAgg {
                            total_work_minutes: 0,
                            late_night_minutes: 0,
                            drive_minutes: 0,
                            cargo_minutes: 0,
                            unko_nos: Vec::new(),
                            segments: Vec::new(),
                            overlap_drive_minutes: 0,
                            overlap_cargo_minutes: 0,
                            overlap_break_minutes: 0,
                            overlap_restraint_minutes: 0,
                            ot_late_night_minutes: 0,
                            from_multi_op: true,
                        });
                    entry.from_multi_op = true;
                    entry.total_work_minutes += ds.work_minutes;
                    entry.late_night_minutes += ds.late_night_minutes;
                    entry.drive_minutes += ds.drive_minutes;
                    entry.cargo_minutes += ds.cargo_minutes;
                    if !entry.unko_nos.contains(&row.unko_no) {
                        entry.unko_nos.push(row.unko_no.clone());
                    }
                    entry.segments.push(SegRec {
                        start_at: ds.start,
                        end_at: ds.end,
                    });
                }
            }
        }
    }

    // ot_late_night計算用: ドライバー×日のDrive/Cargoイベント時刻リスト
    let mut day_work_events: HashMap<
        (String, NaiveDate, NaiveTime),
        Vec<(NaiveDateTime, NaiveDateTime)>,
    > = HashMap::new();

    // ---- イベント直接集計（秒単位→分変換） ----
    {
        let mut driver_unko_map: HashMap<String, Vec<String>> = HashMap::new();
        for ((driver_cd, _, _), agg) in &day_map {
            let entry = driver_unko_map.entry(driver_cd.clone()).or_default();
            for u in &agg.unko_nos {
                if !entry.contains(u) {
                    entry.push(u.clone());
                }
            }
        }

        for (driver_cd, unko_nos) in &driver_unko_map {
            let mut day_drive_secs: HashMap<(NaiveDate, NaiveTime), i64> = HashMap::new();
            let mut day_cargo_secs: HashMap<(NaiveDate, NaiveTime), i64> = HashMap::new();
            let mut day_break_secs: HashMap<(NaiveDate, NaiveTime), i64> = HashMap::new();
            let mut day_late_night: HashMap<(NaiveDate, NaiveTime), i32> = HashMap::new();

            for unko_no in unko_nos {
                if let Some(events) = kudgivt_by_unko.get(unko_no) {
                    let boundary_opt = multi_op_boundaries.get(unko_no);

                    for evt in events {
                        let dur = evt.duration_minutes.unwrap_or(0);
                        if dur <= 0 {
                            continue;
                        }

                        let evt_start_trunc = trunc_min(evt.start_at);
                        let evt_end = evt_start_trunc + chrono::Duration::minutes(dur as i64);

                        let mut parts: Vec<(NaiveDateTime, NaiveDateTime, i64)> = Vec::new();
                        if let Some(&boundary) = boundary_opt {
                            if evt_start_trunc < boundary && evt_end > boundary {
                                let before_secs = (boundary - evt_start_trunc).num_seconds();
                                let after_secs = (evt_end - boundary).num_seconds();
                                parts.push((evt_start_trunc, boundary, before_secs));
                                parts.push((boundary, evt_end, after_secs));
                            } else {
                                parts.push((evt_start_trunc, evt_end, dur as i64 * 60));
                            }
                        } else {
                            parts.push((evt_start_trunc, evt_end, dur as i64 * 60));
                        }

                        for (part_start, part_end, part_secs) in &parts {
                            let (event_date, event_start_time) = unko_segments
                                .get(unko_no)
                                .and_then(|segs| {
                                    segs.iter()
                                        .find(|(start, end, _, _)| {
                                            *part_start >= *start && *part_start < *end
                                        })
                                        .or_else(|| {
                                            segs.iter()
                                                .find(|(start, _, _, _)| *part_start < *start)
                                        })
                                        .or_else(|| segs.last())
                                        .map(|(_, _, wd, st)| (*wd, *st))
                                })
                                .unwrap_or((
                                    part_start.date(),
                                    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                                ));

                            let cls = classifications.get(&evt.event_cd);
                            match cls {
                                Some(EventClass::Drive) => {
                                    *day_drive_secs
                                        .entry((event_date, event_start_time))
                                        .or_insert(0) += part_secs;
                                }
                                Some(EventClass::Cargo) => {
                                    *day_cargo_secs
                                        .entry((event_date, event_start_time))
                                        .or_insert(0) += part_secs;
                                }
                                Some(EventClass::Break) => {
                                    *day_break_secs
                                        .entry((event_date, event_start_time))
                                        .or_insert(0) += part_secs;
                                }
                                _ => {}
                            }
                            match cls {
                                Some(EventClass::Drive) | Some(EventClass::Cargo) => {
                                    let night = calc_late_night_mins(*part_start, *part_end);
                                    if night > 0 {
                                        *day_late_night
                                            .entry((event_date, event_start_time))
                                            .or_insert(0) += night;
                                    }
                                    day_work_events
                                        .entry((driver_cd.clone(), event_date, event_start_time))
                                        .or_default()
                                        .push((*part_start, *part_end));
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }

            for ((date, st), secs) in &day_drive_secs {
                if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), *date, *st)) {
                    agg.drive_minutes = (*secs / 60) as i32;
                }
            }
            for ((date, st), secs) in &day_cargo_secs {
                if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), *date, *st)) {
                    agg.cargo_minutes = (*secs / 60) as i32;
                }
            }
            for ((date, st), _) in day_drive_secs
                .iter()
                .chain(day_cargo_secs.iter())
                .chain(day_break_secs.iter())
            {
                if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), *date, *st)) {
                    let d = day_drive_secs.get(&(*date, *st)).copied().unwrap_or(0);
                    let c = day_cargo_secs.get(&(*date, *st)).copied().unwrap_or(0);
                    let b = day_break_secs.get(&(*date, *st)).copied().unwrap_or(0);
                    agg.total_work_minutes = ((d + c + b) / 60) as i32;
                }
            }
            for ((date, st), night) in &day_late_night {
                if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), *date, *st)) {
                    agg.late_night_minutes = *night;
                }
            }
            for ((dc, _date, _st), agg) in day_map.iter_mut() {
                if dc == driver_cd && !day_late_night.contains_key(&(*_date, *_st)) {
                    agg.late_night_minutes = 0;
                }
            }
            // ot_late_night（実働ベース: 累計Drive/Cargo 480分到達後の深夜時間）
            for ((date, st), _night) in &day_late_night {
                if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), *date, *st)) {
                    let ot_night = if let Some(events) =
                        day_work_events.get(&(driver_cd.clone(), *date, *st))
                    {
                        let mut sorted = events.clone();
                        sorted.sort_by_key(|&(s, _)| s);
                        calc_ot_late_night_from_events(&sorted)
                    } else {
                        0
                    };
                    agg.ot_late_night_minutes = ot_night;
                }
            }
        }
    }

    let ferry_info = FerryInfo::from_zip_files(&zip_files, &kudgivt_by_unko);
    post_process_day_map(
        &mut day_map,
        &mut workday_boundaries,
        &mut day_work_events,
        &kudgivt_by_unko,
        &classifications,
        &kudguri_rows,
        &ferry_info,
    );

    // ---- CsvDriverData に変換 ----
    let mut driver_map: HashMap<String, String> = HashMap::new();
    for row in &kudguri_rows {
        driver_map
            .entry(row.driver_cd.clone())
            .or_insert_with(|| row.driver_name.clone());
    }

    let month_start = NaiveDate::from_ymd_opt(target_year, target_month, 1).unwrap();
    let month_end = if target_month == 12 {
        NaiveDate::from_ymd_opt(target_year + 1, 1, 1).unwrap()
    } else {
        NaiveDate::from_ymd_opt(target_year, target_month + 1, 1).unwrap()
    } - chrono::Duration::days(1);

    let mut result = Vec::new();

    for (driver_cd, driver_name) in &driver_map {
        let mut days = Vec::new();
        let mut cumulative = 0i32;
        let mut total_drive = 0i32;
        let mut total_restraint = 0i32;
        let mut total_actual_work = 0i32;
        let mut total_overtime = 0i32;
        let mut total_late_night = 0i32;

        let mut current_date = month_start;
        while current_date <= month_end {
            let day_entries: Vec<_> = day_map
                .iter()
                .filter(|((dc, d, _), _)| dc == driver_cd && *d == current_date)
                .collect();

            if day_entries.is_empty() {
                days.push(CsvDayRow {
                    date: format!("{}月{}日", current_date.month(), current_date.day()),
                    is_holiday: true,
                    start_time: String::new(),
                    end_time: String::new(),
                    drive: String::new(),
                    overlap_drive: String::new(),
                    cargo: String::new(),
                    overlap_cargo: String::new(),
                    break_time: String::new(),
                    overlap_break: String::new(),
                    subtotal: String::new(),
                    overlap_subtotal: String::new(),
                    total: String::new(),
                    cumulative: fmt_min(cumulative),
                    rest: String::new(),
                    actual_work: String::new(),
                    overtime: String::new(),
                    late_night: String::new(),
                    ot_late_night: String::new(),
                    remarks: String::new(),
                });
            } else {
                let mut sorted_entries: Vec<_> = day_entries;
                sorted_entries.sort_by_key(|((_, _, st), _)| *st);

                for ((_, _, _st), agg) in &sorted_entries {
                    let day_drive = agg.drive_minutes;
                    let day_cargo = agg.cargo_minutes;
                    let day_restraint = agg.total_work_minutes;
                    let overlap_restraint = agg.overlap_restraint_minutes;
                    let day_total = day_restraint + overlap_restraint;

                    cumulative += day_restraint;

                    let actual_work = day_drive + day_cargo;
                    let ot_ln = agg.ot_late_night_minutes;
                    let total_ot = (actual_work - 480).max(0);
                    let overtime = (total_ot - ot_ln).max(0);

                    let fmt_trunc_time = |dt: NaiveDateTime| -> String {
                        format!("{}:{:02}", dt.hour(), dt.minute())
                    };
                    let wb = workday_boundaries.get(&(driver_cd.clone(), current_date, *_st));
                    let start_time = wb
                        .map(|(wd_start, _)| fmt_trunc_time(*wd_start))
                        .or_else(|| {
                            agg.segments
                                .iter()
                                .map(|s| s.start_at)
                                .min()
                                .map(fmt_trunc_time)
                        })
                        .unwrap_or_default();
                    let seg_max_end = agg.segments.iter().map(|s| s.end_at).max();
                    let end_time = match (wb, seg_max_end) {
                        (Some((wd_start, wd_end)), Some(seg_end))
                            if wd_start.date() != wd_end.date()
                                && seg_end.date() == wd_start.date() =>
                        {
                            fmt_trunc_time(*wd_end)
                        }
                        (_, Some(seg_end)) => fmt_trunc_time(seg_end),
                        (Some((_, wd_end)), None) => fmt_trunc_time(*wd_end),
                        _ => String::new(),
                    };

                    let standard_late_night = (agg.late_night_minutes - ot_ln).max(0);

                    total_drive += day_drive;
                    total_restraint += day_restraint;
                    total_actual_work += actual_work;
                    total_overtime += overtime;
                    total_late_night += standard_late_night;

                    days.push(CsvDayRow {
                        date: format!("{}月{}日", current_date.month(), current_date.day()),
                        is_holiday: false,
                        start_time,
                        end_time,
                        drive: fmt_min(day_drive),
                        overlap_drive: fmt_min(agg.overlap_drive_minutes),
                        cargo: fmt_min(day_cargo),
                        overlap_cargo: fmt_min(agg.overlap_cargo_minutes),
                        break_time: fmt_min((day_restraint - day_drive - day_cargo).max(0)),
                        overlap_break: fmt_min(agg.overlap_break_minutes),
                        subtotal: fmt_min(day_restraint),
                        overlap_subtotal: fmt_min(overlap_restraint),
                        total: fmt_min(day_total),
                        cumulative: fmt_min(cumulative),
                        rest: String::new(),
                        actual_work: fmt_min(actual_work),
                        overtime: fmt_min(overtime),
                        late_night: fmt_min(standard_late_night),
                        ot_late_night: fmt_min(ot_ln),
                        remarks: String::new(),
                    });
                }
            }

            current_date += chrono::Duration::days(1);
        }

        result.push(CsvDriverData {
            driver_name: driver_name.clone(),
            driver_cd: driver_cd.clone(),
            days,
            total_drive: fmt_min(total_drive),
            total_cargo: String::new(),
            total_break: String::new(),
            total_restraint: fmt_min(total_restraint),
            total_actual_work: fmt_min(total_actual_work),
            total_overtime: fmt_min(total_overtime),
            total_late_night: fmt_min(total_late_night),
            total_ot_late_night: String::new(),
        });
    }

    Ok(result)
}
