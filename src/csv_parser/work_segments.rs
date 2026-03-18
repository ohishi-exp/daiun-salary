use chrono::{NaiveDate, NaiveDateTime, Timelike};
use std::collections::HashMap;

use super::kudgivt::KudgivtRow;

/// 改善基準告示の休息基準（分）
const REST_THRESHOLD_PRINCIPAL: i32 = 540; // 原則: 連続540分以上
const REST_SPLIT_MIN: i32 = 180; // 分割特例: 1回180分以上
const REST_SPLIT_2_TOTAL: i32 = 600; // 2分割: 合計600分以上
const REST_SPLIT_3_TOTAL: i32 = 720; // 3分割: 合計720分以上
const MAX_WORK_HOURS: i64 = 24 * 60; // 24時間ルール（分）

/// 1つの勤務日（始業〜終業）
#[derive(Debug, Clone)]
pub struct Workday {
    pub start: NaiveDateTime, // 始業
    pub end: NaiveDateTime,   // 終業
    pub date: NaiveDate,      // 帰属日 = start.date()
}

/// ドライバーの全302イベントから勤務日（始業〜終業）を決定する
///
/// ルール（改善基準告示 令和6年4月）:
/// 1. 休息基準を満たした場合 → 休息開始で終業、休息終了後の次の拘束開始で新規始業
///    - [原則] 連続540分以上
///    - [分割特例] 1回180分以上の休息の累計が 2分割=600分 / 3分割=720分
/// 2. 始業から24h経過で休息基準未達 → 強制日締め
///
/// - `rest_events`: 302イベント（時系列ソート済み）
/// - `first_start`: 最初の拘束開始（出社日時等）
/// - `last_end`: 最後の拘束終了
/// - `is_long_distance`: 宿泊を伴う長距離貨物運送（例外基準: 480分）
pub fn determine_workdays(
    rest_events: &[(NaiveDateTime, i32)], // (start_at, duration_minutes)
    first_start: NaiveDateTime,
    last_end: NaiveDateTime,
    is_long_distance: bool,
) -> Vec<Workday> {
    let mut workdays = Vec::new();
    let mut current_start = first_start;
    let mut split_rests: Vec<i32> = Vec::new(); // 分割特例用: 180分以上の休息を蓄積
    tracing::debug!(
        "determine_workdays: first_start={}, last_end={}, rest_events={}",
        first_start,
        last_end,
        rest_events.len()
    );

    for &(rest_start, rest_duration) in rest_events {
        let rest_end = rest_start + chrono::Duration::minutes(rest_duration as i64);

        // 24時間ルール: 始業から24h経過していたら強制日締め（複数回分割の可能性）
        let mut handled_by_24h = false;
        loop {
            let max_end = current_start + chrono::Duration::minutes(MAX_WORK_HOURS);
            if rest_start >= max_end {
                // 休息開始が24h後より後 → 24h境界で強制分割
                workdays.push(Workday {
                    start: current_start,
                    end: max_end,
                    date: current_start.date(),
                });
                current_start = max_end;
                split_rests.clear();
            } else if rest_start < max_end && rest_end > max_end {
                // 24hマークが休息の途中に落ちる場合:
                // 「始業から24時間後が休息中なら休息の開始が終業になる」
                // 休息終了が新しい始業
                workdays.push(Workday {
                    start: current_start,
                    end: rest_start,
                    date: current_start.date(),
                });
                current_start = rest_end;
                split_rests.clear();
                handled_by_24h = true;
                break;
            } else {
                break;
            }
        }
        if handled_by_24h {
            continue;
        }

        // 原則: 連続540分以上
        // 長距離例外: 最後の休息のみ480分以上（運行終了後の休息基準）
        let is_last_rest = rest_events
            .last()
            .map(|&(s, _)| s == rest_start)
            .unwrap_or(false);
        let threshold = if is_long_distance && is_last_rest {
            480
        } else {
            REST_THRESHOLD_PRINCIPAL
        };
        if rest_duration >= threshold {
            workdays.push(Workday {
                start: current_start,
                end: rest_start,
                date: current_start.date(),
            });
            current_start = rest_end;
            split_rests.clear();
            continue;
        }

        // 分割特例: 180分以上の休息を蓄積してチェック
        if rest_duration >= REST_SPLIT_MIN {
            split_rests.push(rest_duration);
            let total: i32 = split_rests.iter().sum();
            let threshold = match split_rests.len() {
                2 => REST_SPLIT_2_TOTAL,
                n if n >= 3 => REST_SPLIT_3_TOTAL,
                _ => i32::MAX, // 1回だけでは分割特例不成立
            };
            if total >= threshold {
                workdays.push(Workday {
                    start: current_start,
                    end: rest_start,
                    date: current_start.date(),
                });
                current_start = rest_end;
                split_rests.clear();
                continue;
            }
        }
    }

    // 最後の勤務日（24hルールで複数日に分割される可能性あり）
    tracing::debug!(
        "determine_workdays: after loop, current_start={}, last_end={}, workdays_so_far={}",
        current_start,
        last_end,
        workdays.len()
    );
    for (i, wd) in workdays.iter().enumerate() {
        if wd.date >= chrono::NaiveDate::from_ymd_opt(2026, 2, 17).unwrap()
            && wd.date <= chrono::NaiveDate::from_ymd_opt(2026, 2, 20).unwrap()
        {
            tracing::debug!(
                "  workday[{}]: date={}, start={}, end={}",
                i,
                wd.date,
                wd.start,
                wd.end
            );
        }
    }
    while current_start < last_end {
        let max_end = current_start + chrono::Duration::minutes(MAX_WORK_HOURS);
        if last_end > max_end {
            workdays.push(Workday {
                start: current_start,
                end: max_end,
                date: current_start.date(),
            });
            current_start = max_end;
        } else {
            workdays.push(Workday {
                start: current_start,
                end: last_end,
                date: current_start.date(),
            });
            break;
        }
    }

    workdays
}

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
/// 日跨ぎ対応: 0:00境界で分割して各日の深夜時間を合算する
pub fn calc_late_night_mins(day_start: NaiveDateTime, day_end: NaiveDateTime) -> i32 {
    // 同一日 or ちょうど翌日0:00 → 単一日ロジック
    if day_end.date() == day_start.date()
        || (day_end.date() == day_start.date().succ_opt().unwrap()
            && day_end.hour() == 0
            && day_end.minute() == 0)
    {
        return calc_late_night_single_day(day_start, day_end);
    }
    // 日跨ぎ: 0:00境界で分割して合算
    let mut total = 0i32;
    let mut cur = day_start;
    while cur.date() < day_end.date() {
        let midnight = cur.date().succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap();
        total += calc_late_night_single_day(cur, midnight);
        cur = midnight;
    }
    total += calc_late_night_single_day(cur, day_end);
    total
}

/// 同一日内（day_endが翌日0:00含む）の深夜時間を計算
fn calc_late_night_single_day(day_start: NaiveDateTime, day_end: NaiveDateTime) -> i32 {
    let mut total = 0i32;
    let start_h = day_start.hour() * 60 + day_start.minute();
    let end_h = if day_end.date() > day_start.date() && day_end.hour() == 0 && day_end.minute() == 0
    {
        1440u32
    } else {
        day_end.hour() * 60 + day_end.minute()
    };
    // 0:00〜5:00 (0〜300分)
    let early_start = start_h;
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
        .map(|e| {
            let dur = e.duration_minutes.unwrap_or(0);
            if dur > 0 {
                e.start_at + chrono::Duration::minutes(dur as i64)
            } else {
                // duration=0 のイベント（運行開始/終了等）は start_at を使う
                e.start_at
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
            let (drive, cargo) =
                sum_events_in_range(&labor_events, classifications, current_start, rest_start);
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
        let (drive, cargo) =
            sum_events_in_range(&labor_events, classifications, current_start, actual_end);
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

/// 24時間超のセグメントを24h境界で強制分割する（休息未取得時例外）
/// 改善基準告示: 集計開始時刻の24時間後を日締め時刻とする
///
/// workday_ends: determine_workdaysのwd.end一覧（始業基準の24h境界）
/// workday_endsがある場合、seg基準ではなく始業基準で分割する
pub fn split_segments_at_24h_with_workdays(
    segments: Vec<WorkSegment>,
    workday_ends: &[NaiveDateTime],
) -> Vec<WorkSegment> {
    let max_mins = 24 * 60i64;
    let mut result = Vec::new();
    for seg in segments {
        let total_mins = (seg.end - seg.start).num_minutes();
        // workday境界がセグメント内にあれば分割（24h未満でも）
        // 境界は分単位に切り捨て済みなので、seg側も分単位で比較
        let seg_start_trunc = seg.start.with_second(0).unwrap_or(seg.start);
        let seg_end_trunc = seg.end.with_second(0).unwrap_or(seg.end);
        let wd_boundaries: Vec<NaiveDateTime> = workday_ends
            .iter()
            .filter(|&&b| b > seg_start_trunc && b < seg_end_trunc)
            .copied()
            .collect();
        if total_mins <= max_mins && wd_boundaries.is_empty() {
            result.push(seg);
            continue;
        }
        let mut boundaries = wd_boundaries;
        if boundaries.is_empty() {
            // workday境界がない場合はseg基準で24h分割（従来動作）
            let mut cur_start = seg.start;
            while cur_start < seg.end {
                let cur_end = (cur_start + chrono::Duration::minutes(max_mins)).min(seg.end);
                boundaries.push(cur_end);
                cur_start = cur_end;
            }
            boundaries.pop(); // seg.endは不要
        }
        boundaries.sort();

        let total_labor = seg.labor_minutes as f64;
        let total_drive = seg.drive_minutes as f64;
        let total_cargo = seg.cargo_minutes as f64;
        let total_wall = total_mins as f64;
        let mut cur_start = seg.start;
        for boundary in boundaries.iter().chain(std::iter::once(&seg.end)) {
            if *boundary <= cur_start {
                continue;
            }
            let cur_end = *boundary;
            let chunk_mins = (cur_end - cur_start).num_minutes() as f64;
            let ratio = chunk_mins / total_wall;
            result.push(WorkSegment {
                start: cur_start,
                end: cur_end,
                labor_minutes: (total_labor * ratio).round() as i32,
                drive_minutes: (total_drive * ratio).round() as i32,
                cargo_minutes: (total_cargo * ratio).round() as i32,
            });
            cur_start = cur_end;
        }
    }
    result
}

/// 互換ラッパー: workday境界なしの24h分割
pub fn split_segments_at_24h(segments: Vec<WorkSegment>) -> Vec<WorkSegment> {
    split_segments_at_24h_with_workdays(segments, &[])
}

/// 指定範囲内のイベントを運転/荷役に分けて duration_minutes を合計
pub fn sum_events_in_range(
    events: &[&&KudgivtRow],
    classifications: &HashMap<String, EventClass>,
    range_start: NaiveDateTime,
    range_end: NaiveDateTime,
) -> (i32, i32) {
    let mut drive = 0i32;
    let mut cargo = 0i32;
    for e in events
        .iter()
        .filter(|e| e.start_at >= range_start && e.start_at < range_end)
    {
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
        // 秒を切り捨ててHH:MM精度に揃える（web地球号互換）
        let start_trunc = seg.start.with_second(0).unwrap_or(seg.start);
        let end_trunc = seg.end.with_second(0).unwrap_or(seg.end);
        let total_work_mins = (end_trunc - start_trunc).num_minutes().max(1) as f64;

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

            // 秒を切り捨ててHH:MM精度に揃える（web地球号互換）
            let day_start_trunc = day_start.with_second(0).unwrap_or(day_start);
            let day_end_trunc = day_end.with_second(0).unwrap_or(day_end);
            let work_mins = (day_end_trunc - day_start_trunc).num_minutes() as i32;
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

    fn make_event(
        unko_no: &str,
        start_at: NaiveDateTime,
        event_cd: &str,
        duration: Option<i32>,
    ) -> KudgivtRow {
        KudgivtRow {
            unko_no: unko_no.to_string(),
            reading_date: NaiveDate::from_ymd_opt(2026, 2, 27).unwrap(),
            driver_cd: "2".to_string(),
            driver_name: "テスト".to_string(),
            crew_role: 1,
            start_at,
            end_at: duration.map(|d| start_at + chrono::Duration::minutes(d as i64)),
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
        let events = vec![make_event("001", dt(2026, 2, 24, 10, 0), "110", Some(300))];
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
            make_event("001", dt(2026, 2, 24, 10, 0), "110", Some(240)), // 運転 4h
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
            make_event("001", dt(2026, 2, 24, 10, 25), "110", Some(324)), // 運転
            make_event("001", dt(2026, 2, 24, 14, 40), "302", Some(1123)), // 休息 ~18.7h
            make_event("001", dt(2026, 2, 25, 9, 30), "110", Some(200)),  // 運転
            make_event("001", dt(2026, 2, 25, 21, 31), "302", Some(780)), // 休息 13h
            make_event("001", dt(2026, 2, 26, 10, 30), "110", Some(300)), // 運転
            make_event("001", dt(2026, 2, 26, 21, 25), "302", Some(572)), // 休息 ~9.5h
            make_event("001", dt(2026, 2, 27, 7, 0), "110", Some(400)),   // 運転
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
        assert_eq!(
            calc_late_night_mins(dt(2026, 1, 1, 22, 0), dt(2026, 1, 1, 23, 30),),
            90
        );

        // 0:00〜5:00
        assert_eq!(
            calc_late_night_mins(dt(2026, 1, 1, 0, 0), dt(2026, 1, 1, 5, 0),),
            300
        );

        // 昼間のみ
        assert_eq!(
            calc_late_night_mins(dt(2026, 1, 1, 8, 0), dt(2026, 1, 1, 17, 0),),
            0
        );
    }

    #[test]
    fn test_24h_mark_during_rest() {
        // 始業: 2/21 08:30
        // 休息: 2/22 06:00〜15:00 (540min)
        // 24hマーク: 2/22 08:30 → 休息の途中
        // 期待: workday1 ends at 06:00 (rest_start), workday2 starts at 15:00 (rest_end)
        let rest_events = vec![(dt(2026, 2, 22, 6, 0), 540)];
        let first_start = dt(2026, 2, 21, 8, 30);
        let last_end = dt(2026, 2, 22, 20, 0);
        let workdays = determine_workdays(&rest_events, first_start, last_end, false);
        assert_eq!(workdays.len(), 2);
        assert_eq!(workdays[0].start, dt(2026, 2, 21, 8, 30));
        assert_eq!(workdays[0].end, dt(2026, 2, 22, 6, 0)); // 休息開始 = 終業
        assert_eq!(workdays[1].start, dt(2026, 2, 22, 15, 0)); // 休息終了 = 始業
        assert_eq!(workdays[1].end, dt(2026, 2, 22, 20, 0));
    }

    #[test]
    fn test_24h_mark_during_short_rest() {
        // 始業: 2/21 08:30
        // 休息: 2/22 07:00〜11:00 (240min, < 540min)
        // 24hマーク: 2/22 08:30 → 休息の途中
        // 短い休息でも24hルールで日締めされる
        let rest_events = vec![(dt(2026, 2, 22, 7, 0), 240)];
        let first_start = dt(2026, 2, 21, 8, 30);
        let last_end = dt(2026, 2, 22, 20, 0);
        let workdays = determine_workdays(&rest_events, first_start, last_end, false);
        assert_eq!(workdays.len(), 2);
        assert_eq!(workdays[0].end, dt(2026, 2, 22, 7, 0)); // 休息開始 = 終業
        assert_eq!(workdays[1].start, dt(2026, 2, 22, 11, 0)); // 休息終了 = 始業
    }

    #[test]
    fn test_24h_mark_after_short_rest_no_split() {
        // 1039ケース: 383min休息が24hマーク前に終了 → 新ルール不発動
        // 始業: 2/21 08:30
        // 休息: 2/21 23:17〜2/22 05:40 (383min)
        // 24hマーク: 2/22 08:30 → 休息は05:40に終了済み
        let rest_events = vec![(dt(2026, 2, 21, 23, 17), 383)];
        let first_start = dt(2026, 2, 21, 8, 30);
        let last_end = dt(2026, 2, 22, 20, 20);
        let workdays = determine_workdays(&rest_events, first_start, last_end, false);
        // 383min < 540min → 休息による分割なし
        // 24hルールで08:30に強制分割
        assert_eq!(workdays.len(), 2);
        assert_eq!(workdays[0].end, dt(2026, 2, 22, 8, 30)); // 24h境界
        assert_eq!(workdays[1].start, dt(2026, 2, 22, 8, 30)); // 24h境界から開始
    }
}
