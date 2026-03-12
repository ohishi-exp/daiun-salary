use chrono::{NaiveDate, NaiveDateTime};

/// KUDGURI.csv の1行をパースした結果
#[derive(Debug, Clone)]
pub struct KudguriRow {
    pub unko_no: String,
    pub reading_date: NaiveDate,
    pub operation_date: Option<NaiveDate>,
    pub office_cd: String,
    pub office_name: String,
    pub vehicle_cd: String,
    pub vehicle_name: String,
    pub driver_cd: String,
    pub driver_name: String,
    pub crew_role: i32,
    pub departure_at: Option<NaiveDateTime>,
    pub return_at: Option<NaiveDateTime>,
    pub garage_out_at: Option<NaiveDateTime>,
    pub garage_in_at: Option<NaiveDateTime>,
    pub meter_start: Option<f64>,
    pub meter_end: Option<f64>,
    pub total_distance: Option<f64>,
    pub drive_time_general: Option<i32>,
    pub drive_time_highway: Option<i32>,
    pub drive_time_bypass: Option<i32>,
    pub safety_score: Option<f64>,
    pub economy_score: Option<f64>,
    pub total_score: Option<f64>,
    pub raw_data: serde_json::Value,
}

/// KUDGURI.csv ヘッダー名とカラムインデックスのマッピング
struct ColumnIndex {
    unko_no: usize,
    reading_date: usize,
    operation_date: Option<usize>,
    office_cd: usize,
    office_name: usize,
    vehicle_cd: usize,
    vehicle_name: usize,
    driver_cd: usize,
    driver_name: usize,
    crew_role: usize,
    departure_at: Option<usize>,
    return_at: Option<usize>,
    garage_out_at: Option<usize>,
    garage_in_at: Option<usize>,
    meter_start: Option<usize>,
    meter_end: Option<usize>,
    total_distance: Option<usize>,
    drive_time_general: Option<usize>,
    drive_time_highway: Option<usize>,
    drive_time_bypass: Option<usize>,
    safety_score: Option<usize>,
    economy_score: Option<usize>,
    total_score: Option<usize>,
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
    let office_cd = require_col(headers, "事業所CD", &mut missing);
    let office_name = require_col(headers, "事業所名", &mut missing);
    let vehicle_cd = require_col(headers, "車輌CD", &mut missing);
    let vehicle_name = require_col(headers, "車輌名", &mut missing);
    let driver_cd = require_col(headers, "乗務員CD1", &mut missing);
    let driver_name = require_col(headers, "乗務員名１", &mut missing);
    let crew_role = require_col(headers, "対象乗務員区分", &mut missing);

    if !missing.is_empty() {
        return Err(format!("missing required columns: {}", missing.join(", ")));
    }

    Ok(ColumnIndex {
        unko_no: unko_no.unwrap(),
        reading_date: reading_date.unwrap(),
        operation_date: find_col(headers, "運行日"),
        office_cd: office_cd.unwrap(),
        office_name: office_name.unwrap(),
        vehicle_cd: vehicle_cd.unwrap(),
        vehicle_name: vehicle_name.unwrap(),
        driver_cd: driver_cd.unwrap(),
        driver_name: driver_name.unwrap(),
        crew_role: crew_role.unwrap(),
        departure_at: find_col(headers, "出社日時"),
        return_at: find_col(headers, "退社日時"),
        garage_out_at: find_col(headers, "出庫日時"),
        garage_in_at: find_col(headers, "帰庫日時"),
        meter_start: find_col(headers, "出庫メーター"),
        meter_end: find_col(headers, "帰庫メーター"),
        total_distance: find_col(headers, "総走行距離"),
        drive_time_general: find_col(headers, "一般道運転時間"),
        drive_time_highway: find_col(headers, "高速道運転時間"),
        drive_time_bypass: find_col(headers, "バイパス運転時間"),
        safety_score: find_col(headers, "安全評価点"),
        economy_score: find_col(headers, "経済評価点"),
        total_score: find_col(headers, "総合評価点"),
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
    // "2026/02/27 00:00:00" or "2026/02/27 0:00:00" or "2026/02/27"
    let date_part = s.split_whitespace().next().unwrap_or(s);
    NaiveDate::parse_from_str(date_part, "%Y/%m/%d").ok()
}

fn parse_datetime(s: &str) -> Option<NaiveDateTime> {
    // "2026/02/24 10:13:11"
    NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y/%m/%d %k:%M:%S"))
        .ok()
}

fn parse_f64(s: &str) -> Option<f64> {
    s.parse::<f64>().ok()
}

fn parse_i32(s: &str) -> Option<i32> {
    s.parse::<i32>().ok()
}

/// KUDGURI.csv テキスト全体をパースして KudguriRow のリストを返す
pub fn parse_kudguri(csv_text: &str) -> Result<Vec<KudguriRow>, anyhow::Error> {
    let mut lines = csv_text.lines();
    let header_line = lines.next().ok_or_else(|| anyhow::anyhow!("empty CSV"))?;
    let headers: Vec<&str> = header_line.split(',').collect();
    let col_idx =
        build_column_index(&headers).map_err(|e| anyhow::anyhow!(e))?;

    let mut rows = Vec::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(',').collect();

        // Build raw_data as JSON object with all columns
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

        let crew_role_str = get_field(&fields, col_idx.crew_role);
        let crew_role = crew_role_str.parse::<i32>().unwrap_or(1);

        rows.push(KudguriRow {
            unko_no,
            reading_date,
            operation_date: get_opt_field(&fields, col_idx.operation_date)
                .and_then(parse_date),
            office_cd: get_field(&fields, col_idx.office_cd).to_string(),
            office_name: get_field(&fields, col_idx.office_name).to_string(),
            vehicle_cd: get_field(&fields, col_idx.vehicle_cd).to_string(),
            vehicle_name: get_field(&fields, col_idx.vehicle_name).to_string(),
            driver_cd: get_field(&fields, col_idx.driver_cd).to_string(),
            driver_name: get_field(&fields, col_idx.driver_name).to_string(),
            crew_role,
            departure_at: get_opt_field(&fields, col_idx.departure_at)
                .and_then(parse_datetime),
            return_at: get_opt_field(&fields, col_idx.return_at)
                .and_then(parse_datetime),
            garage_out_at: get_opt_field(&fields, col_idx.garage_out_at)
                .and_then(parse_datetime),
            garage_in_at: get_opt_field(&fields, col_idx.garage_in_at)
                .and_then(parse_datetime),
            meter_start: get_opt_field(&fields, col_idx.meter_start)
                .and_then(parse_f64),
            meter_end: get_opt_field(&fields, col_idx.meter_end)
                .and_then(parse_f64),
            total_distance: get_opt_field(&fields, col_idx.total_distance)
                .and_then(parse_f64),
            drive_time_general: get_opt_field(&fields, col_idx.drive_time_general)
                .and_then(parse_i32),
            drive_time_highway: get_opt_field(&fields, col_idx.drive_time_highway)
                .and_then(parse_i32),
            drive_time_bypass: get_opt_field(&fields, col_idx.drive_time_bypass)
                .and_then(parse_i32),
            safety_score: get_opt_field(&fields, col_idx.safety_score)
                .and_then(parse_f64),
            economy_score: get_opt_field(&fields, col_idx.economy_score)
                .and_then(parse_f64),
            total_score: get_opt_field(&fields, col_idx.total_score)
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
    fn test_parse_kudguri_sample() {
        let csv = "運行NO,読取日,運行日,事業所CD,事業所名,車輌CD,車輌名,乗務員CD1,乗務員名１,対象乗務員区分,出社日時,退社日時,出庫日時,帰庫日時,出庫メーター,帰庫メーター,総走行距離,一般道運転時間,高速道運転時間,バイパス運転時間,安全評価点,経済評価点,総合評価点\n\
2602241025060000000272,2026/02/27 00:00:00,2026/02/24 0:00:00,1,本社,272,帯広100け272,2,梅津　政弘,1,2026/02/24 10:13:11,2026/02/27 16:00:54,2026/02/24 10:25:06,2026/02/27 15:48:59,449854.1,451990.6,2136.5,108,1687,0,98,99,98";

        let rows = parse_kudguri(csv).unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.unko_no, "2602241025060000000272");
        assert_eq!(row.reading_date, NaiveDate::from_ymd_opt(2026, 2, 27).unwrap());
        assert_eq!(row.office_cd, "1");
        assert_eq!(row.office_name, "本社");
        assert_eq!(row.vehicle_cd, "272");
        assert_eq!(row.driver_cd, "2");
        assert_eq!(row.driver_name, "梅津　政弘");
        assert_eq!(row.crew_role, 1);
        assert!((row.total_distance.unwrap() - 2136.5).abs() < 0.01);
        assert_eq!(row.drive_time_general, Some(108));
        assert_eq!(row.drive_time_highway, Some(1687));
        assert!((row.total_score.unwrap() - 98.0).abs() < 0.01);
    }

    #[test]
    fn test_missing_columns_error_message() {
        let csv = "運行NO,読取日\ndata1,data2";
        let err = parse_kudguri(csv).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("missing required columns"), "got: {msg}");
        assert!(msg.contains("事業所CD"), "got: {msg}");
        assert!(msg.contains("乗務員CD1"), "got: {msg}");
        assert!(msg.contains("対象乗務員区分"), "got: {msg}");
        // 存在するカラムは含まれない
        assert!(!msg.contains("運行NO"), "got: {msg}");
        assert!(!msg.contains("読取日"), "got: {msg}");
    }
}
