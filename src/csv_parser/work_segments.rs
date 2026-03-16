use chrono::{NaiveDate, NaiveDateTime, Timelike};
use std::collections::HashMap;

use super::kudgivt::KudgivtRow;

/// イベント分類
#[derive(Debug, Clone, PartialEq)]
pub enum EventClass {
    Drive,     // 運転 (110)
    Cargo,     // 荷役 (202=積み, 203=降し)
    RestSplit, // 勤務区間の区切り (302=休息)
    Break,     // 拘束内だが労働時間外 (301=休憩)
    Ignore,    // 無視 (101=実車, 103=高速道, 412=アイドリング等)
}

/// 1つの連続勤務区間
#[derive(Debug, Clone)]
pub struct WorkSegment {
    pub start: NaiveDateTime,
    pub end: NaiveDateTime,
    pub labor_minutes: i32,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
}

/// 日別に分割された勤務区間
#[derive(Debug, Clone)]
pub struct DailyWorkSegment {
    pub date: NaiveDate,
    pub start: NaiveDateTime,
    pub end: NaiveDateTime,
    pub work_minutes: i32,
    pub labor_minutes: i32,
    pub late_night_minutes: i32,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
}

/// day_start〜day_end 間の深夜時間（22:00〜翌5:00）を分単位で返す
/// 同一日内の区間を想定。day_endが翌日0:00の場合は1440(24:00)として扱う
pub fn calc_late_night_mins(day_start: NaiveDateTime, day_end: NaiveDateTime) -> i32 {
    let mut total = 0i32;
    let start_h = day_start.hour() * 60 + day_start.minute();
    // day_endが翌日の0:00の場合、1440(24:00)として扱う
    let end_h = if day_end.date() > day_start.date() && day_end.hour() == 0 && day_end.minute() == 0 {
        1440u32
    } else {
        day_end.hour() * 60 + day_end.minute()
    };
    // 0:00〜5:00 (0〜300分)
    let early_start = start_h.max(0);
    let early_end = end_h.min(300);
    if early_end > early_start {
        total += (early_end - early_start) as i32;
    }
    // 22:00〜24:00 (1320〜1440分)
    let late_start = start_h.max(1320);
    let late_end = end_h.min(1440);
    if late_end > late_start {
        total += (late_end - late_start) as i32;
    }
    total
}

/// KUDGIVT イベント列と分類マップから、KUDGURI 1運行を勤務区間に分割する
///
/// - `departure_at`: 出社日時 (KUDGURI)
/// - `return_at`: 退社日時 (KUDGURI)
/// - `events`: この運行の全KUDGIVTイベント
/// - `classifications`: event_cd → EventClass のマップ
pub fn split_by_rest(
    departure_at: NaiveDateTime,
    return_at: NaiveDateTime,
    events: &[&KudgivtRow],
    classifications: &HashMap<String, EventClass>,
) -> Vec<WorkSegment> {
    // 休息(rest_split)イベントを start_at 昇順でソート
    let mut rest_events: Vec<&&KudgivtRow> = events
        .iter()
        .filter(|e| {
            classifications
                .get(&e.event_cd)
                .map(|c| *c == EventClass::RestSplit)
                .unwrap_or(false)
        })
        .collect();
    rest_events.sort_by_key(|e| e.start_at);

    // 労働(drive/cargo)イベントを start_at 昇順でソート
    let mut labor_events: Vec<&&KudgivtRow> = events
        .iter()
        .filter(|e| {
            classifications
                .get(&e.event_cd)
                .map(|c| *c == EventClass::Drive || *c == EventClass::Cargo)
                .unwrap_or(false)
        })
        .collect();
    labor_events.sort_by_key(|e| e.start_at);

    // 実際の終了時刻 = イベントの最終終了時刻（なければreturn_at）
    let actual_end = events
        .iter()
        .filter_map(|e| {
            let dur = e.duration_minutes.unwrap_or(0);
            if dur > 0 {
                Some(e.start_at + chrono::Duration::minutes(dur as i64))
            } else {
                // duration=0 のイベント（運行開始/終了等）は start_at を使う
                Some(e.start_at)
            }
        })
        .max()
        .unwrap_or(return_at);

    let mut segments = Vec::new();
    let mut current_start = departure_at;

    for rest in &rest_events {
        let rest_start = rest.start_at;
        let duration = rest.duration_minutes.unwrap_or(0);
        let rest_end = rest_start + chrono::Duration::minutes(duration as i64);

        if rest_start > current_start {
            let (drive, cargo) = sum_events_in_range(&labor_events, classifications, current_start, rest_start);
            segments.push(WorkSegment {
                start: current_start,
                end: rest_start,
                labor_minutes: drive + cargo,
                drive_minutes: drive,
                cargo_minutes: cargo,
            });
        }

        current_start = rest_end.min(actual_end);
    }

    // 最後の区間
    if current_start < actual_end {
        let (drive, cargo) = sum_events_in_range(&labor_events, classifications, current_start, actual_end);
        segments.push(WorkSegment {
            start: current_start,
            end: actual_end,
            labor_minutes: drive + cargo,
            drive_minutes: drive,
            cargo_minutes: cargo,
        });
    }

    segments
}

/// 指定範囲内のイベントを運転/荷役に分けて duration_minutes を合計
fn sum_events_in_range(
    events: &[&&KudgivtRow],
    classifications: &HashMap<String, EventClass>,
    range_start: NaiveDateTime,
    range_end: NaiveDateTime,
) -> (i32, i32) {
    let mut drive = 0i32;
    let mut cargo = 0i32;
    for e in events.iter().filter(|e| e.start_at >= range_start && e.start_at < range_end) {
        let dur = e.duration_minutes.unwrap_or(0);
        match classifications.get(&e.event_cd) {
            Some(EventClass::Drive) => drive += dur,
            Some(EventClass::Cargo) => cargo += dur,
            _ => {}
        }
    }
    (drive, cargo)
}

/// 勤務区間を0:00境界で日別に分割する
pub fn split_segments_by_day(segments: &[WorkSegment]) -> Vec<DailyWorkSegment> {
    let mut daily = Vec::new();

    for seg in segments {
        let mut current = seg.start.date();
        let end_date = seg.end.date();
        let total_work_mins = (seg.end - seg.start).num_minutes().max(1) as f64;

        while current <= end_date {
            let day_start = if current == seg.start.date() {
                seg.start
            } else {
                current.and_hms_opt(0, 0, 0).unwrap()
            };
            let day_end = if current == end_date {
                seg.end
            } else {
                (current + chrono::Duration::days(1))
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
            };

            // 秒を四捨五入して分に変換（CSVのイベント合計は整数分なので切り捨てだとズレる）
            let work_mins = ((day_end - day_start).num_seconds() as f64 / 60.0).round() as i32;
            if work_mins <= 0 {
                current += chrono::Duration::days(1);
                continue;
            }

            let ratio = work_mins as f64 / total_work_mins;
            let labor_mins = (seg.labor_minutes as f64 * ratio).round() as i32;
            let drive_mins = (seg.drive_minutes as f64 * ratio).round() as i32;
            let cargo_mins = (seg.cargo_minutes as f64 * ratio).round() as i32;
            let late_night = calc_late_night_mins(day_start, day_end);

            daily.push(DailyWorkSegment {
                date: current,
                start: day_start,
                end: day_end,
                work_minutes: work_mins,
                labor_minutes: labor_mins,
                late_night_minutes: late_night,
                drive_minutes: drive_mins,
                cargo_minutes: cargo_mins,
            });

            current += chrono::Duration::days(1);
        }
    }

    daily
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn make_classifications() -> HashMap<String, EventClass> {
        let mut m = HashMap::new();
        m.insert("110".to_string(), EventClass::Drive);
        m.insert("202".to_string(), EventClass::Cargo);
        m.insert("203".to_string(), EventClass::Cargo);
        m.insert("302".to_string(), EventClass::RestSplit);
        m.insert("301".to_string(), EventClass::Break);
        m.insert("101".to_string(), EventClass::Ignore);
        m.insert("103".to_string(), EventClass::Ignore);
        m.insert("412".to_string(), EventClass::Ignore);
        m
    }

    fn make_event(unko_no: &str, start_at: NaiveDateTime, event_cd: &str, duration: Option<i32>) -> KudgivtRow {
        KudgivtRow {
            unko_no: unko_no.to_string(),
            reading_date: NaiveDate::from_ymd_opt(2026, 2, 27).unwrap(),
            driver_cd: "2".to_string(),
            driver_name: "テスト".to_string(),
            crew_role: 1,
            start_at,
            event_cd: event_cd.to_string(),
            event_name: "test".to_string(),
            duration_minutes: duration,
            section_distance: None,
            raw_data: serde_json::Value::Null,
        }
    }

    fn dt(y: i32, m: u32, d: u32, h: u32, mi: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, mi, 0)
            .unwrap()
    }

    #[test]
    fn test_no_rest_events_single_segment() {
        let dep = dt(2026, 2, 24, 10, 0);
        let ret = dt(2026, 2, 24, 18, 0);
        let events = vec![
            make_event("001", dt(2026, 2, 24, 10, 0), "110", Some(300)),
        ];
        let refs: Vec<&KudgivtRow> = events.iter().collect();
        let cls = make_classifications();

        let segments = split_by_rest(dep, ret, &refs, &cls);
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].start, dep);
        assert_eq!(segments[0].end, ret);
        assert_eq!(segments[0].labor_minutes, 300);
    }

    #[test]
    fn test_single_rest_splits_into_two() {
        let dep = dt(2026, 2, 24, 10, 0);
        let ret = dt(2026, 2, 25, 18, 0);
        let events = vec![
            make_event("001", dt(2026, 2, 24, 10, 0), "110", Some(240)),  // 運転 4h
            make_event("001", dt(2026, 2, 24, 14, 0), "302", Some(600)), // 休息 10h
            make_event("001", dt(2026, 2, 25, 0, 0), "110", Some(480)),  // 運転 8h
        ];
        let refs: Vec<&KudgivtRow> = events.iter().collect();
        let cls = make_classifications();

        let segments = split_by_rest(dep, ret, &refs, &cls);
        assert_eq!(segments.len(), 2);
        // 区間1: 10:00 → 14:00
        assert_eq!(segments[0].start, dt(2026, 2, 24, 10, 0));
        assert_eq!(segments[0].end, dt(2026, 2, 24, 14, 0));
        assert_eq!(segments[0].labor_minutes, 240);
        // 区間2: 00:00(休息終了) → 18:00
        assert_eq!(segments[1].start, dt(2026, 2, 25, 0, 0));
        assert_eq!(segments[1].end, dt(2026, 2, 25, 18, 0));
        assert_eq!(segments[1].labor_minutes, 480);
    }

    #[test]
    fn test_multi_day_operation_with_real_data() {
        // 2/24 10:13出社 → 2/27 16:00退社
        let dep = dt(2026, 2, 24, 10, 13);
        let ret = dt(2026, 2, 27, 16, 0);
        let events = vec![
            make_event("001", dt(2026, 2, 24, 10, 25), "110", Some(324)),   // 運転
            make_event("001", dt(2026, 2, 24, 14, 40), "302", Some(1123)),  // 休息 ~18.7h
            make_event("001", dt(2026, 2, 25, 9, 30), "110", Some(200)),    // 運転
            make_event("001", dt(2026, 2, 25, 21, 31), "302", Some(780)),   // 休息 13h
            make_event("001", dt(2026, 2, 26, 10, 30), "110", Some(300)),   // 運転
            make_event("001", dt(2026, 2, 26, 21, 25), "302", Some(572)),   // 休息 ~9.5h
            make_event("001", dt(2026, 2, 27, 7, 0), "110", Some(400)),     // 運転
        ];
        let refs: Vec<&KudgivtRow> = events.iter().collect();
        let cls = make_classifications();

        let segments = split_by_rest(dep, ret, &refs, &cls);
        assert_eq!(segments.len(), 4);

        // 区間1: 10:13 → 14:40 (4h27m)
        assert_eq!(segments[0].start, dt(2026, 2, 24, 10, 13));
        assert_eq!(segments[0].end, dt(2026, 2, 24, 14, 40));

        // 区間2: 14:40 + 1123min = ~09:23翌日 → 21:31
        // 1123 min = 18h43m → 14:40 + 18:43 = 2/25 09:23
        assert_eq!(segments[1].end, dt(2026, 2, 25, 21, 31));

        // 区間3: 21:31 + 780min = ~10:31翌日 → 21:25
        assert_eq!(segments[2].end, dt(2026, 2, 26, 21, 25));

        // 区間4: → 16:00
        assert_eq!(segments[3].end, dt(2026, 2, 27, 16, 0));

        // 拘束時間は24時間にはならない
        for seg in &segments {
            let mins = (seg.end - seg.start).num_minutes();
            assert!(mins < 24 * 60, "segment should be < 24h, got {}min", mins);
        }
    }

    #[test]
    fn test_split_segments_by_day() {
        let segments = vec![WorkSegment {
            start: dt(2026, 2, 24, 22, 0),
            end: dt(2026, 2, 25, 6, 0),
            labor_minutes: 400,
            drive_minutes: 300,
            cargo_minutes: 100,
        }];

        let daily = split_segments_by_day(&segments);
        assert_eq!(daily.len(), 2);

        // Day 1: 22:00 → 00:00 = 120min
        assert_eq!(daily[0].date, NaiveDate::from_ymd_opt(2026, 2, 24).unwrap());
        assert_eq!(daily[0].work_minutes, 120);
        assert_eq!(daily[0].late_night_minutes, 120); // 22:00-24:00 is all late night

        // Day 2: 00:00 → 06:00 = 360min
        assert_eq!(daily[1].date, NaiveDate::from_ymd_opt(2026, 2, 25).unwrap());
        assert_eq!(daily[1].work_minutes, 360);
        assert_eq!(daily[1].late_night_minutes, 300); // 00:00-05:00

        // labor按分: 120/480*400=100, 360/480*400=300
        assert_eq!(daily[0].labor_minutes, 100);
        assert_eq!(daily[1].labor_minutes, 300);
    }

    #[test]
    fn test_calc_late_night_mins() {
        // 22:00〜翌05:00 の全深夜帯
        assert_eq!(calc_late_night_mins(
            dt(2026, 1, 1, 22, 0),
            dt(2026, 1, 1, 23, 30),
        ), 90);

        // 0:00〜5:00
        assert_eq!(calc_late_night_mins(
            dt(2026, 1, 1, 0, 0),
            dt(2026, 1, 1, 5, 0),
        ), 300);

        // 昼間のみ
        assert_eq!(calc_late_night_mins(
            dt(2026, 1, 1, 8, 0),
            dt(2026, 1, 1, 17, 0),
        ), 0);
    }
}
