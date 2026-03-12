use chrono::{NaiveDate, NaiveDateTime};

/// KUDGIVT.csv の1行をパースした結果
#[derive(Debug, Clone)]
pub struct KudgivtRow {
    pub unko_no: String,
    pub reading_date: NaiveDate,
    pub driver_cd: String,
    pub driver_name: String,
    pub crew_role: i32,
    pub start_at: NaiveDateTime,
    pub event_cd: String,
    pub event_name: String,
    pub duration_minutes: Option<i32>,
    pub section_distance: Option<f64>,
    pub raw_data: serde_json::Value,
}

struct ColumnIndex {
    unko_no: usize,
    reading_date: usize,
    driver_cd: usize,
    driver_name: usize,
    crew_role: usize,
    start_at: usize,
    event_cd: usize,
    event_name: usize,
    duration_minutes: Option<usize>,
    section_distance: Option<usize>,
}

fn find_col(headers: &[&str], name: &str) -> Option<usize> {
    headers.iter().position(|h| h.trim() == name)
}

fn require_col<'a>(headers: &[&str], name: &'a str, missing: &mut Vec<&'a str>) -> Option<usize> {
    let idx = find_col(headers, name);
    if idx.is_none() {
        missing.push(name);
    }
    idx
}

fn build_column_index(headers: &[&str]) -> Result<ColumnIndex, String> {
    let mut missing = Vec::new();

    let unko_no = require_col(headers, "運行NO", &mut missing);
    let reading_date = require_col(headers, "読取日", &mut missing);
    let driver_cd = require_col(headers, "乗務員CD1", &mut missing);
    let driver_name = require_col(headers, "乗務員名１", &mut missing);
    let crew_role = require_col(headers, "対象乗務員区分", &mut missing);
    let start_at = require_col(headers, "開始日時", &mut missing);
    let event_cd = require_col(headers, "イベントCD", &mut missing);
    let event_name = require_col(headers, "イベント名", &mut missing);

    if !missing.is_empty() {
        return Err(format!("missing required columns: {}", missing.join(", ")));
    }

    Ok(ColumnIndex {
        unko_no: unko_no.unwrap(),
        reading_date: reading_date.unwrap(),
        driver_cd: driver_cd.unwrap(),
        driver_name: driver_name.unwrap(),
        crew_role: crew_role.unwrap(),
        start_at: start_at.unwrap(),
        event_cd: event_cd.unwrap(),
        event_name: event_name.unwrap(),
        duration_minutes: find_col(headers, "区間時間"),
        section_distance: find_col(headers, "区間距離"),
    })
}

fn get_field<'a>(fields: &'a [&str], idx: usize) -> &'a str {
    fields.get(idx).map(|s| s.trim()).unwrap_or("")
}

fn get_opt_field<'a>(fields: &'a [&str], idx: Option<usize>) -> Option<&'a str> {
    idx.and_then(|i| fields.get(i).map(|s| s.trim()))
        .filter(|s| !s.is_empty())
}

fn parse_date(s: &str) -> Option<NaiveDate> {
    let date_part = s.split_whitespace().next().unwrap_or(s);
    NaiveDate::parse_from_str(date_part, "%Y/%m/%d").ok()
}

fn parse_datetime(s: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y/%m/%d %k:%M:%S"))
        .ok()
}

fn parse_i32(s: &str) -> Option<i32> {
    s.parse::<i32>().ok()
}

fn parse_f64(s: &str) -> Option<f64> {
    s.parse::<f64>().ok()
}

/// KUDGIVT.csv テキスト全体をパースして KudgivtRow のリストを返す
pub fn parse_kudgivt(csv_text: &str) -> Result<Vec<KudgivtRow>, anyhow::Error> {
    let mut lines = csv_text.lines();
    let header_line = lines.next().ok_or_else(|| anyhow::anyhow!("empty CSV"))?;
    let headers: Vec<&str> = header_line.split(',').collect();
    let col_idx = build_column_index(&headers).map_err(|e| anyhow::anyhow!(e))?;

    let mut rows = Vec::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(',').collect();

        let mut raw_map = serde_json::Map::new();
        for (i, header) in headers.iter().enumerate() {
            let val = fields.get(i).map(|s| s.trim()).unwrap_or("");
            raw_map.insert(
                header.trim().to_string(),
                serde_json::Value::String(val.to_string()),
            );
        }

        let unko_no = get_field(&fields, col_idx.unko_no).to_string();
        let reading_date_str = get_field(&fields, col_idx.reading_date);
        let reading_date = parse_date(reading_date_str)
            .ok_or_else(|| anyhow::anyhow!("invalid reading_date: {}", reading_date_str))?;

        let start_at_str = get_field(&fields, col_idx.start_at);
        let start_at = match parse_datetime(start_at_str) {
            Some(dt) => dt,
            None => continue, // skip rows with invalid datetime
        };

        let crew_role_str = get_field(&fields, col_idx.crew_role);
        let crew_role = crew_role_str.parse::<i32>().unwrap_or(1);

        rows.push(KudgivtRow {
            unko_no,
            reading_date,
            driver_cd: get_field(&fields, col_idx.driver_cd).to_string(),
            driver_name: get_field(&fields, col_idx.driver_name).to_string(),
            crew_role,
            start_at,
            event_cd: get_field(&fields, col_idx.event_cd).to_string(),
            event_name: get_field(&fields, col_idx.event_name).to_string(),
            duration_minutes: get_opt_field(&fields, col_idx.duration_minutes)
                .and_then(parse_i32),
            section_distance: get_opt_field(&fields, col_idx.section_distance)
                .and_then(parse_f64),
            raw_data: serde_json::Value::Object(raw_map),
        });
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_kudgivt_sample() {
        let csv = "運行NO,読取日,事業所CD,事業所名,車輌CD,車輌名,乗務員CD1,乗務員名１,対象乗務員区分,開始日時,イベントCD,イベント名,開始走行距離,終了走行距離,区間時間,区間距離,開始市町村CD,開始市町村名,終了市町村CD,終了市町村名,開始場所CD,開始場所名,終了場所CD,終了場所名\n\
2602241025060000000272,2026/02/27 00:00:00,1,本社,272,帯広100け272,2,梅津　政弘,1,2026/02/24 14:40:56,302,休息,248.9,250.1,1123,1.2,1203,小樽市築港,1203,小樽市築港,,,,";

        let rows = parse_kudgivt(csv).unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.unko_no, "2602241025060000000272");
        assert_eq!(row.event_cd, "302");
        assert_eq!(row.event_name, "休息");
        assert_eq!(row.duration_minutes, Some(1123));
        assert!((row.section_distance.unwrap() - 1.2).abs() < 0.01);
        assert_eq!(row.driver_cd, "2");
    }

    #[test]
    fn test_missing_columns_error_message() {
        let csv = "運行NO,読取日\ndata1,data2";
        let err = parse_kudgivt(csv).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("missing required columns"), "got: {msg}");
        assert!(msg.contains("乗務員CD1"), "got: {msg}");
        assert!(msg.contains("イベントCD"), "got: {msg}");
        assert!(!msg.contains("運行NO"), "got: {msg}");
    }
}
