#![allow(dead_code)]
//! 拘束時間管理表 CSV 比較 CLI
//!
//! Usage:
//!   cargo run --bin compare -- <csv1> <csv2>           # 2ファイル比較
//!   cargo run --bin compare -- <csv1> <csv2> -d 1026   # ドライバー指定
//!   cargo run --bin compare -- <csv1>                  # 1ファイル内サマリー
//!   cargo run --bin compare -- <zip> <csv>             # ZIP→計算→CSV比較

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::process;

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use daiun_salary::csv_parser;
use daiun_salary::csv_parser::kudgivt::KudgivtRow;
use daiun_salary::csv_parser::kudguri::KudguriRow;
use daiun_salary::csv_parser::work_segments::{self, EventClass, calc_late_night_mins};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: compare <csv1|zip> [csv2] [-d driver_cd]");
        process::exit(2);
    }

    let mut files = Vec::new();
    let mut driver_filter: Option<String> = None;
    let mut i = 1;
    while i < args.len() {
        if args[i] == "-d" || args[i] == "--driver" {
            i += 1;
            if i < args.len() {
                driver_filter = Some(args[i].clone());
            }
        } else {
            files.push(args[i].clone());
        }
        i += 1;
    }

    if files.is_empty() {
        eprintln!("Error: CSVファイルを指定してください");
        process::exit(2);
    }

    // ZIP ファイルを検出
    let zip_idx = files.iter().position(|f| f.ends_with(".zip"));
    if let Some(zi) = zip_idx {
        // ZIP + 参照CSV モード
        if files.len() < 2 {
            eprintln!("Error: ZIP比較にはZIPファイルと参照CSVの2つが必要です");
            process::exit(2);
        }
        let csv_idx = if zi == 0 { 1 } else { 0 };
        let zip_path = &files[zi];
        let csv_path = &files[csv_idx];

        let zip_bytes = fs::read(zip_path).unwrap_or_else(|e| {
            eprintln!("Error: {} を読めません: {}", zip_path, e);
            process::exit(2);
        });
        let csv_bytes = fs::read(csv_path).unwrap_or_else(|e| {
            eprintln!("Error: {} を読めません: {}", csv_path, e);
            process::exit(2);
        });

        let ref_drivers = parse_restraint_csv(&csv_bytes).unwrap_or_else(|e| {
            eprintln!("Error: 参照CSVパースエラー: {}", e);
            process::exit(2);
        });

        // 参照CSVから対象年月を推定
        let (target_year, target_month) = detect_year_month(&ref_drivers);

        let sys_drivers = process_zip(&zip_bytes, target_year, target_month).unwrap_or_else(|e| {
            eprintln!("Error: ZIP処理エラー: {}", e);
            process::exit(2);
        });

        compare_drivers(&ref_drivers, &sys_drivers, &driver_filter);
        return;
    }

    // 従来のCSVモード
    let csv1_bytes = fs::read(&files[0]).unwrap_or_else(|e| {
        eprintln!("Error: {} を読めません: {}", files[0], e);
        process::exit(2);
    });
    let drivers1 = parse_restraint_csv(&csv1_bytes).unwrap_or_else(|e| {
        eprintln!("Error: CSV1パースエラー: {}", e);
        process::exit(2);
    });

    if files.len() == 1 {
        // 1ファイルモード: 各ドライバーのサマリー表示
        for d in &drivers1 {
            if let Some(ref f) = driver_filter {
                if &d.driver_cd != f { continue; }
            }
            println!("=== {} ({}) ===", d.driver_name, d.driver_cd);
            println!("  稼働日数: {}", d.days.iter().filter(|r| !r.is_holiday).count());
            println!("  運転合計: {}", d.total_drive);
            println!("  拘束合計: {}", d.total_restraint);
            println!("  実働合計: {}", d.total_actual_work);
            println!("  時間外:   {}", d.total_overtime);
            println!("  深夜:     {}", d.total_late_night);
            println!();
            for day in &d.days {
                if day.is_holiday { continue; }
                println!("  {} 始業:{} 終業:{} 運転:{} 小計:{} 合計:{} 累計:{} 実働:{} 時間外:{} 深夜:{}",
                    day.date, day.start_time, day.end_time,
                    day.drive, day.subtotal, day.total, day.cumulative,
                    day.actual_work, day.overtime, day.late_night);
            }
            println!();
        }
        return;
    }

    // 2ファイル比較モード
    let csv2_bytes = fs::read(&files[1]).unwrap_or_else(|e| {
        eprintln!("Error: {} を読めません: {}", files[1], e);
        process::exit(2);
    });
    let drivers2 = parse_restraint_csv(&csv2_bytes).unwrap_or_else(|e| {
        eprintln!("Error: CSV2パースエラー: {}", e);
        process::exit(2);
    });

    compare_drivers(&drivers1, &drivers2, &driver_filter);
}

fn compare_drivers(drivers1: &[CsvDriverData], drivers2: &[CsvDriverData], driver_filter: &Option<String>) {
    let mut total_diffs = 0;
    let mut has_match = false;

    for d1 in drivers1 {
        if let Some(ref f) = driver_filter {
            if &d1.driver_cd != f { continue; }
        }
        let d2 = drivers2.iter().find(|d| d.driver_cd == d1.driver_cd);
        let Some(d2) = d2 else {
            println!("\x1b[33m=== {} ({}) — CSV2に該当ドライバーなし ===\x1b[0m", d1.driver_name, d1.driver_cd);
            continue;
        };
        has_match = true;

        let diffs = detect_diffs_csv(&d1.days, &d2.days);

        println!("=== {} ({}) ===", d1.driver_name, d1.driver_cd);
        if diffs.is_empty() {
            println!("  \x1b[32m差分なし\x1b[0m");
        } else {
            for d in &diffs {
                println!("  \x1b[31m{} {}: csv1={} csv2={}\x1b[0m", d.date, d.field, d.csv_val, d.sys_val);
            }
            println!("  差分: {}件", diffs.len());
            total_diffs += diffs.len();
        }

        // 合計行の比較
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
                println!("  \x1b[31m{}: csv1={} csv2={}\x1b[0m", label, a, b);
                total_diffs += 1;
            }
        }
        println!();
    }

    if !has_match {
        if let Some(ref f) = driver_filter {
            eprintln!("Warning: ドライバー {} が見つかりません", f);
        }
    }

    println!("合計差分: {}件", total_diffs);
    process::exit(if total_diffs > 0 { 1 } else { 0 });
}

// ========== ZIP → インメモリ計算 ==========

/// 参照CSVの日付データから対象年月を推定
fn detect_year_month(drivers: &[CsvDriverData]) -> (i32, u32) {
    for d in drivers {
        for day in &d.days {
            if day.is_holiday { continue; }
            // "2月1日" → month=2
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

fn default_classifications() -> HashMap<String, EventClass> {
    let mut m = HashMap::new();
    m.insert("201".to_string(), EventClass::Drive);   // 走行
    m.insert("202".to_string(), EventClass::Cargo);    // 積み
    m.insert("203".to_string(), EventClass::Cargo);    // 降し
    m.insert("302".to_string(), EventClass::RestSplit); // 休息
    m.insert("301".to_string(), EventClass::Break);    // 休憩
    m
}

fn group_operations_into_work_days(rows: &[KudguriRow]) -> HashMap<String, NaiveDate> {
    const REST_THRESHOLD_MINUTES: i64 = 540;
    const MAX_WORK_DAY_MINUTES: i64 = 1440;

    let mut unko_work_date: HashMap<String, NaiveDate> = HashMap::new();
    let mut driver_rows: HashMap<String, Vec<&KudguriRow>> = HashMap::new();
    for row in rows {
        if !row.driver_cd.is_empty() {
            driver_rows.entry(row.driver_cd.clone()).or_default().push(row);
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

            let mut new_day = false;
            if let (Some(shigyo), Some(prev_end)) = (current_shigyo, last_end) {
                let gap_minutes = (dep - prev_end).num_minutes();
                let since_shigyo_minutes = (dep - shigyo).num_minutes();
                if gap_minutes >= REST_THRESHOLD_MINUTES {
                    new_day = true;
                } else if since_shigyo_minutes >= MAX_WORK_DAY_MINUTES {
                    new_day = true;
                }
            } else {
                new_day = true;
            }

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

/// KUDGFRY.csv をパースしてフェリー時間(分)を返す
fn parse_ferry_minutes(zip_files: &[(String, Vec<u8>)]) -> HashMap<String, i32> {
    let mut ferry_map = HashMap::new();
    for (name, bytes) in zip_files {
        if !name.to_uppercase().contains("KUDGFRY") { continue; }
        let text = csv_parser::decode_shift_jis(bytes);
        for line in text.lines().skip(1) {
            let cols: Vec<&str> = line.split(',').collect();
            if cols.len() <= 11 { continue; }
            let unko_no = cols[0].trim().to_string();
            if let (Some(start), Some(end)) = (
                NaiveDateTime::parse_from_str(cols[10].trim(), "%Y/%m/%d %H:%M:%S").ok()
                    .or_else(|| NaiveDateTime::parse_from_str(cols[10].trim(), "%Y/%m/%d %k:%M:%S").ok()),
                NaiveDateTime::parse_from_str(cols[11].trim(), "%Y/%m/%d %H:%M:%S").ok()
                    .or_else(|| NaiveDateTime::parse_from_str(cols[11].trim(), "%Y/%m/%d %k:%M:%S").ok()),
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

/// 秒を切り捨てて分精度に揃える
fn trunc_min(dt: NaiveDateTime) -> NaiveDateTime {
    dt.with_second(0).unwrap_or(dt)
}

fn fmt_min(val: i32) -> String {
    if val == 0 { return String::new(); }
    format!("{}:{:02}", val / 60, val.abs() % 60)
}

/// ZIP を処理して CsvDriverData を生成
fn process_zip(zip_bytes: &[u8], target_year: i32, target_month: u32) -> Result<Vec<CsvDriverData>, String> {
    let zip_files = csv_parser::extract_zip(zip_bytes).map_err(|e| format!("ZIP展開エラー: {e}"))?;

    // KUDGURI.csv / KUDGIVT.csv を探してパース
    let kudguri_bytes = zip_files.iter().find(|(n, _)| n.to_uppercase().contains("KUDGURI"))
        .ok_or("KUDGURI.csv が見つかりません")?;
    let kudgivt_bytes = zip_files.iter().find(|(n, _)| n.to_uppercase().contains("KUDGIVT"))
        .ok_or("KUDGIVT.csv が見つかりません")?;

    let kudguri_text = csv_parser::decode_shift_jis(&kudguri_bytes.1);
    let kudgivt_text = csv_parser::decode_shift_jis(&kudgivt_bytes.1);

    let kudguri_rows = csv_parser::kudguri::parse_kudguri(&kudguri_text)
        .map_err(|e| format!("KUDGURIパースエラー: {e}"))?;
    let kudgivt_rows = csv_parser::kudgivt::parse_kudgivt(&kudgivt_text)
        .map_err(|e| format!("KUDGIVTパースエラー: {e}"))?;

    let ferry_minutes = parse_ferry_minutes(&zip_files);
    let classifications = default_classifications();
    let _unko_work_date = group_operations_into_work_days(&kudguri_rows);

    // Group KUDGIVT by unko_no
    let mut kudgivt_by_unko: HashMap<String, Vec<&KudgivtRow>> = HashMap::new();
    for row in &kudgivt_rows {
        kudgivt_by_unko.entry(row.unko_no.clone()).or_default().push(row);
    }

    // ---- DayAgg: 日別集計 ----
    struct DayAgg {
        total_work_minutes: i32,
        late_night_minutes: i32,
        drive_minutes: i32,
        cargo_minutes: i32,
        unko_nos: Vec<String>,
        segments: Vec<SegRec>,
        overlap_drive_minutes: i32,
        overlap_cargo_minutes: i32,
        overlap_break_minutes: i32,
        overlap_restraint_minutes: i32,
        ot_late_night_minutes: i32,
    }
    #[derive(Clone)]
    struct SegRec {
        start_at: NaiveDateTime,
        end_at: NaiveDateTime,
    }

    // ドライバーごとの全workday境界を収集（始業/終業表示用）
    // キー: (driver_cd, workday.date, workday.start.time()) → (workday.start, workday.end)
    let mut workday_boundaries: HashMap<(String, NaiveDate, NaiveTime), (NaiveDateTime, NaiveDateTime)> = HashMap::new();

    // キー: (driver_cd, work_date, start_time)
    let mut day_map: HashMap<(String, NaiveDate, NaiveTime), DayAgg> = HashMap::new();

    // unko_no → セグメント情報 (seg.start, seg.end, work_date, start_time)
    let mut unko_segments: HashMap<String, Vec<(NaiveDateTime, NaiveDateTime, NaiveDate, NaiveTime)>> = HashMap::new();

    for row in &kudguri_rows {
        match (row.departure_at, row.return_at) {
            (Some(dep), Some(ret)) if ret > dep => {
                let events = kudgivt_by_unko.get(&row.unko_no);
                let event_slice: Vec<&KudgivtRow> = events
                    .map(|e| e.iter().copied().collect())
                    .unwrap_or_default();

                let segments = work_segments::split_by_rest(dep, ret, &event_slice, &classifications);
                let segments = work_segments::split_segments_at_24h(segments);
                let daily_segments = work_segments::split_segments_by_day(&segments);

                // workday境界を決定
                let rest_events_for_unko: Vec<(NaiveDateTime, i32)> = event_slice.iter()
                    .filter(|e| classifications.get(&e.event_cd) == Some(&EventClass::RestSplit))
                    .filter_map(|e| e.duration_minutes.filter(|&d| d > 0).map(|d| (e.start_at, d)))
                    .collect();
                let workdays = work_segments::determine_workdays(&rest_events_for_unko, dep, ret);

                // workday境界を収集
                for wd in &workdays {
                    workday_boundaries.insert(
                        (row.driver_cd.clone(), wd.date, wd.start.time()),
                        (wd.start, wd.end),
                    );
                }

                let find_start_time = |ts: NaiveDateTime| -> NaiveTime {
                    workdays.iter()
                        .find(|wd| ts >= wd.start && ts < wd.end)
                        .or_else(|| workdays.iter().rev().find(|wd| ts >= wd.start))
                        .map(|wd| wd.start.time())
                        .unwrap_or(dep.time())
                };

                // セグメント情報保存（workday日付を使用）
                let find_workday_date = |start: NaiveDateTime, end: NaiveDateTime| -> NaiveDate {
                    // セグメントが完全にworkday内に収まる場合、そのworkdayの日付を使用
                    workdays.iter()
                        .find(|wd| start >= wd.start && end <= wd.end)
                        .map(|wd| wd.date)
                        .unwrap_or(start.date())
                };
                let seg_entries: Vec<_> = segments.iter()
                    .map(|seg| (seg.start, seg.end, find_workday_date(seg.start, seg.end), find_start_time(seg.start)))
                    .collect();
                unko_segments.insert(row.unko_no.clone(), seg_entries);

                for ds in &daily_segments {
                    // workday内に完全に収まるか確認
                    let work_date = workdays.iter()
                        .find(|wd| ds.start >= wd.start && ds.end <= wd.end)
                        .map(|wd| wd.date)
                        .unwrap_or_else(|| {
                            // 複数workdayに跨る場合はparent segmentの日付
                            let parent_seg = segments.iter()
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
            _ => {
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
                    });

                entry.total_work_minutes += total_drive_mins;
                entry.unko_nos.push(row.unko_no.clone());
            }
        }
    }

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
                    for evt in events {
                        let dur = evt.duration_minutes.unwrap_or(0);
                        if dur <= 0 { continue; }
                        let dur_secs = dur as i64 * 60;

                        let (event_date, event_start_time) = unko_segments.get(unko_no)
                            .and_then(|segs| {
                                segs.iter()
                                    .find(|(start, end, _, _)| evt.start_at >= *start && evt.start_at < *end)
                                    .or_else(|| segs.iter().find(|(start, _, _, _)| evt.start_at < *start))
                                    .or_else(|| segs.last())
                                    .map(|(_, _, wd, st)| (*wd, *st))
                            })
                            .unwrap_or((evt.start_at.date(), NaiveTime::from_hms_opt(0, 0, 0).unwrap()));

                        let cls = classifications.get(&evt.event_cd);
                        match cls {
                            Some(EventClass::Drive) => {
                                *day_drive_secs.entry((event_date, event_start_time)).or_insert(0) += dur_secs;
                            }
                            Some(EventClass::Cargo) => {
                                *day_cargo_secs.entry((event_date, event_start_time)).or_insert(0) += dur_secs;
                            }
                            Some(EventClass::Break) => {
                                *day_break_secs.entry((event_date, event_start_time)).or_insert(0) += dur_secs;
                            }
                            _ => {}
                        }
                        match cls {
                            Some(EventClass::Drive) | Some(EventClass::Cargo) => {
                                let evt_end = evt.start_at + chrono::Duration::minutes(dur as i64);
                                let night = calc_late_night_mins(evt.start_at, evt_end);
                                if night > 0 {
                                    *day_late_night.entry((event_date, event_start_time)).or_insert(0) += night;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }

            // 秒合計→分変換
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
            for ((date, st), _) in day_drive_secs.iter().chain(day_cargo_secs.iter()).chain(day_break_secs.iter()) {
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
            // ot_late_night
            for ((date, st), night) in &day_late_night {
                if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), *date, *st)) {
                    let shigyo = agg.segments.iter().map(|s| s.start_at).min();
                    let ot_night = if let Some(start) = shigyo {
                        let overtime_start = start + chrono::Duration::minutes(480);
                        let night_start_22 = start.date().and_hms_opt(22, 0, 0).unwrap();
                        let night_end_05 = (start.date() + chrono::Duration::days(1))
                            .and_hms_opt(5, 0, 0).unwrap();
                        let effective_night_end = if start.hour() < 5 {
                            start.date().and_hms_opt(5, 0, 0).unwrap()
                        } else {
                            night_end_05
                        };
                        if overtime_start >= effective_night_end {
                            0
                        } else if overtime_start <= night_start_22 {
                            *night
                        } else {
                            *night
                        }
                    } else {
                        0
                    };
                    agg.ot_late_night_minutes = ot_night;
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

        let mut driver_days: HashMap<String, BTreeMap<(NaiveDate, NaiveTime), DayInfo>> = HashMap::new();
        for ((driver_cd, date, st), agg) in &day_map {
            if agg.segments.is_empty() { continue; }
            let start = trunc_min(agg.segments.iter().map(|s| s.start_at).min().unwrap());
            let end = trunc_min(agg.segments.iter().map(|s| s.end_at).max().unwrap());
            driver_days.entry(driver_cd.clone()).or_default()
                .insert((*date, *st), DayInfo { start, end, unko_nos: agg.unko_nos.clone() });
        }

        for (driver_cd, dates_map) in &driver_days {
            let dates: Vec<(NaiveDate, NaiveTime)> = dates_map.keys().copied().collect();
            let mut effective_start: Option<NaiveDateTime> = None;
            let mut prev_end: Option<NaiveDateTime> = None;
            let mut next_day_deduction: Option<(i32, i32, i32, i32)> = None;

            for (idx, &(date, st)) in dates.iter().enumerate() {
                let info = &dates_map[&(date, st)];

                let reset = match prev_end {
                    Some(pe) => (info.start - pe).num_minutes() >= 480,
                    None => true,
                };
                if reset {
                    effective_start = Some(info.start);
                    next_day_deduction = None;
                } else {
                    effective_start = Some(effective_start.unwrap() + chrono::Duration::hours(24));
                }

                if let Some((ded_drive, ded_cargo, ded_restraint, ded_night)) = next_day_deduction.take() {
                    if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), date, st)) {
                        agg.drive_minutes = (agg.drive_minutes - ded_drive).max(0);
                        agg.cargo_minutes = (agg.cargo_minutes - ded_cargo).max(0);
                        agg.total_work_minutes = (agg.total_work_minutes - ded_restraint).max(0);
                        agg.late_night_minutes = (agg.late_night_minutes - ded_night).max(0);
                    }
                }

                let window_end = effective_start.unwrap() + chrono::Duration::hours(24);

                if idx + 1 < dates.len() {
                    let (next_date, next_st) = dates[idx + 1];
                    let next_info = &dates_map[&(next_date, next_st)];

                    let mut ol_drive = 0i32;
                    let mut ol_cargo = 0i32;
                    let mut ol_restraint = 0i32;

                    for unko_no in &next_info.unko_nos {
                        if let Some(events) = kudgivt_by_unko.get(unko_no) {
                            for evt in events {
                                let cls = classifications.get(&evt.event_cd);
                                let dur = evt.duration_minutes.unwrap_or(0);
                                if dur <= 0 { continue; }
                                let evt_start = trunc_min(evt.start_at);
                                if evt_start >= window_end { continue; }
                                let evt_end = evt_start + chrono::Duration::minutes(dur as i64);
                                if evt_end <= info.end { continue; }
                                if evt_start < info.end { continue; }
                                let overlap_start = evt_start.max(next_info.start);
                                let effective_end = evt_end.min(window_end);
                                if effective_end <= overlap_start { continue; }
                                let mins = (effective_end - overlap_start).num_minutes() as i32;
                                if mins <= 0 { continue; }
                                let actual_dur = if mins >= dur { dur } else { mins };
                                match cls {
                                    Some(EventClass::Drive) => ol_drive += actual_dur,
                                    Some(EventClass::Cargo) => ol_cargo += actual_dur,
                                    _ => {}
                                }
                            }
                        }
                    }

                    if next_info.start < window_end {
                        let restraint_end = day_map
                            .get(&(driver_cd.clone(), next_date, next_st))
                            .map(|next_agg| {
                                next_agg.segments.iter()
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
                    let next_resets = next_gap >= 480;

                    if !next_resets && ol_restraint > 0 {
                        let ol_late_night = calc_late_night_mins(next_info.start, window_end);
                        if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), date, st)) {
                            agg.drive_minutes += ol_drive;
                            agg.cargo_minutes += ol_cargo;
                            agg.total_work_minutes += ol_restraint;
                            agg.ot_late_night_minutes = ol_late_night;
                        }
                        next_day_deduction = Some((ol_drive, ol_cargo, ol_restraint, ol_late_night));
                    } else {
                        if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), date, st)) {
                            agg.overlap_drive_minutes = ol_drive;
                            agg.overlap_cargo_minutes = ol_cargo;
                            agg.overlap_break_minutes = (ol_restraint - ol_drive - ol_cargo).max(0);
                            agg.overlap_restraint_minutes = ol_restraint;
                        }
                    }
                }

                prev_end = Some(info.end);
            }
        }
    }

    // ---- フェリー控除 ----
    // KUDGFRY→301イベントマッチング: フェリー対応301の区間時間を特定
    // web地球号は休憩からフェリー301の区間時間を引き、運転=小計-荷役-休憩で逆算
    let mut ferry_break_dur: HashMap<String, i32> = HashMap::new(); // unko_no → ferry 301 event duration
    for (name, bytes) in &zip_files {
        if !name.to_uppercase().contains("KUDGFRY") { continue; }
        let text = csv_parser::decode_shift_jis(bytes);
        for line in text.lines().skip(1) {
            let cols: Vec<&str> = line.split(',').collect();
            if cols.len() <= 11 { continue; }
            let unko_no = cols[0].trim().to_string();
            if let Some(ferry_start) = NaiveDateTime::parse_from_str(cols[10].trim(), "%Y/%m/%d %H:%M:%S").ok()
                .or_else(|| NaiveDateTime::parse_from_str(cols[10].trim(), "%Y/%m/%d %k:%M:%S").ok())
            {
                // この運行の301イベントからフェリー開始時刻に最も近いものを探す
                if let Some(events) = kudgivt_by_unko.get(&unko_no) {
                    let matching_301 = events.iter()
                        .filter(|e| e.event_cd == "301" && e.duration_minutes.unwrap_or(0) > 0)
                        .min_by_key(|e| (e.start_at - ferry_start).num_seconds().abs());
                    if let Some(evt) = matching_301 {
                        let dur = evt.duration_minutes.unwrap_or(0);
                        *ferry_break_dur.entry(unko_no).or_insert(0) += dur;
                    }
                }
            }
        }
    }

    for ((_driver_cd, _date, _st), agg) in day_map.iter_mut() {
        let mut ferry_deduction = 0i32;
        let mut ferry_break_deduction = 0i32;
        for unko in &agg.unko_nos {
            if let Some(&fm) = ferry_minutes.get(unko) {
                ferry_deduction += fm;
            }
            if let Some(&fb) = ferry_break_dur.get(unko) {
                ferry_break_deduction += fb;
            }
        }
        if ferry_deduction > 0 {
            agg.total_work_minutes = (agg.total_work_minutes - ferry_deduction).max(0);
            // 運転 = 小計 - 荷役 - 休憩（web地球号互換）
            // 休憩 = 301イベント合計 - フェリー301の区間時間
            // break_from_events includes ferry 301, so subtract ferry_break_deduction
            // drive_display = drive_from_201 - (ferry_KUDGFRY - ferry_301_event_dur)
            // KUDGFRY四捨五入(71min)と301区間時間(70min)の差を運転から吸収
            agg.drive_minutes = (agg.drive_minutes - ferry_deduction + ferry_break_deduction).max(0);
        }
    }

    // ---- CsvDriverData に変換 ----
    let mut driver_map: HashMap<String, String> = HashMap::new();
    for row in &kudguri_rows {
        driver_map.entry(row.driver_cd.clone())
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
            // この日のday_mapエントリを探す
            let day_entries: Vec<_> = day_map.iter()
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
                // 始業時刻でソート
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

                    // 始業・終業（分切り捨て、web地球号互換）
                    let fmt_trunc_time = |dt: NaiveDateTime| -> String {
                        format!("{}:{:02}", dt.hour(), dt.minute())
                    };
                    let wb = workday_boundaries.get(&(driver_cd.clone(), current_date, *_st));
                    // 始業: workday境界があればそちら、なければセグメントmin
                    let start_time = wb
                        .map(|(wd_start, _)| fmt_trunc_time(*wd_start))
                        .or_else(|| agg.segments.iter().map(|s| s.start_at).min().map(|dt| fmt_trunc_time(dt)))
                        .unwrap_or_default();
                    // 終業: セグメントの最大end_atを優先（実イベント時刻）。
                    // 日跨ぎでセグメントが当日分しかない場合はworkday終了時刻を使用。
                    let seg_max_end = agg.segments.iter().map(|s| s.end_at).max();
                    let end_time = match (wb, seg_max_end) {
                        (Some((wd_start, wd_end)), Some(seg_end))
                            if wd_start.date() != wd_end.date() && seg_end.date() == wd_start.date() =>
                        {
                            // 日跨ぎだがセグメントが当日で終了 → workday.endを使用
                            fmt_trunc_time(*wd_end)
                        }
                        (_, Some(seg_end)) => fmt_trunc_time(seg_end),
                        (Some((_, wd_end)), None) => fmt_trunc_time(*wd_end),
                        _ => String::new(),
                    };

                    total_drive += day_drive;
                    total_restraint += day_restraint;
                    total_actual_work += actual_work;
                    total_overtime += overtime;
                    total_late_night += agg.late_night_minutes;

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
                        late_night: fmt_min(agg.late_night_minutes),
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

// ========== 以下、restraint_report.rs からコピーした純粋関数 ==========

#[derive(Debug, Clone)]
struct CsvDayRow {
    date: String,
    is_holiday: bool,
    start_time: String,
    end_time: String,
    drive: String,
    overlap_drive: String,
    cargo: String,
    overlap_cargo: String,
    break_time: String,
    overlap_break: String,
    subtotal: String,
    overlap_subtotal: String,
    total: String,
    cumulative: String,
    rest: String,
    actual_work: String,
    overtime: String,
    late_night: String,
    ot_late_night: String,
    remarks: String,
}

#[derive(Debug)]
struct CsvDriverData {
    driver_name: String,
    driver_cd: String,
    days: Vec<CsvDayRow>,
    total_drive: String,
    total_cargo: String,
    total_break: String,
    total_restraint: String,
    total_actual_work: String,
    total_overtime: String,
    total_late_night: String,
    total_ot_late_night: String,
}

#[derive(Debug)]
struct DiffItem {
    date: String,
    field: String,
    csv_val: String,
    sys_val: String,
}

fn parse_restraint_csv(bytes: &[u8]) -> Result<Vec<CsvDriverData>, String> {
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
        if line.is_empty() { continue; }

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

        let Some(ref mut driver) = current else { continue; };
        if !in_data { continue; }

        let cols: Vec<&str> = line.split(',').collect();

        if cols.first().map(|s| s.contains("合計")).unwrap_or(false) {
            driver.total_drive = cols.get(3).unwrap_or(&"").to_string();
            driver.total_cargo = cols.get(5).unwrap_or(&"").to_string();
            driver.total_break = cols.get(7).unwrap_or(&"").to_string();
            driver.total_restraint = cols.get(11).unwrap_or(&"").to_string();
            // col[17]=休息合計, col[18]=実働合計, col[19]=時間外合計, col[20]=深夜合計
            driver.total_actual_work = cols.get(18).unwrap_or(&"").to_string();
            driver.total_overtime = cols.get(19).unwrap_or(&"").to_string();
            driver.total_late_night = cols.get(20).unwrap_or(&"").to_string();
            driver.total_ot_late_night = cols.get(21).unwrap_or(&"").to_string();
            in_data = false;
            continue;
        }

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

    if drivers.is_empty() {
        return Err("ドライバーが見つかりません".to_string());
    }

    Ok(drivers)
}

fn detect_diffs_csv(csv_days: &[CsvDayRow], sys_days: &[CsvDayRow]) -> Vec<DiffItem> {
    let mut diffs = Vec::new();
    let normalize_time = |s: &str| -> String {
        let s = s.trim();
        if s.is_empty() { return String::new(); }
        if let Some((h, m)) = s.split_once(':') {
            let h_num: u32 = h.parse().unwrap_or(0);
            format!("{}:{}", h_num, m)
        } else { s.to_string() }
    };

    // 日付ベースでマッチング
    let mut sys_idx = 0;
    for csv_day in csv_days {
        if csv_day.is_holiday { continue; }

        // 同日付のsys_dayを探す
        let sys_day = sys_days[sys_idx..].iter()
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
