use axum::{
    body::Body,
    extract::{Multipart, Path, Query, State},
    http::StatusCode,
    response::Response,
    routing::{post, get},
    Extension, Json, Router,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use chrono::Timelike;
use tokio_stream::StreamExt;
use crate::csv_parser;
use crate::csv_parser::kudguri::{parse_kudguri, KudguriRow};
use crate::csv_parser::kudgivt::{parse_kudgivt, KudgivtRow};
use crate::csv_parser::work_segments::{self, EventClass};
use crate::middleware::auth::AuthUser;
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/upload", post(upload_zip))
}

pub fn internal_router() -> Router<AppState> {
    Router::new()
        .route("/internal/upload", post(internal_upload_zip))
        .route("/internal/store", post(internal_store_zip))
        .route("/internal/rerun/{upload_id}", post(internal_rerun))
        .route("/internal/split-csv/{upload_id}", post(internal_split_csv))
        .route("/internal/download/{upload_id}", get(internal_download))
        .route("/internal/pending", get(list_pending_uploads))
}

pub fn recalculate_router() -> Router<AppState> {
    Router::new()
        .route("/recalculate", post(internal_recalculate_all))
        .route("/recalculate-driver", post(recalculate_driver))
        .route("/split-csv/{upload_id}", post(split_csv_handler))
        .route("/split-csv-all", post(split_csv_all_handler))
        .route("/uploads", get(list_uploads))
}

#[derive(Debug, Serialize)]
pub struct UploadResponse {
    pub upload_id: Uuid,
    pub operations_count: i32,
    pub status: String,
}

async fn upload_zip(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, (StatusCode, String)> {
    let tenant_id = auth_user.tenant_id;

    // Extract ZIP file from multipart
    let (filename, zip_bytes) = extract_file(&mut multipart).await?;

    // Create upload history record
    let upload_id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO upload_history (id, tenant_id, uploaded_by, filename, status)
           VALUES ($1, $2, $3, $4, 'processing')"#,
    )
    .bind(upload_id)
    .bind(tenant_id)
    .bind(auth_user.user_id)
    .bind(&filename)
    .execute(&state.pool)
    .await
    .map_err(internal_err)?;

    // Process ZIP
    match process_zip(&state, tenant_id, upload_id, &filename, &zip_bytes).await {
        Ok(count) => {
            // Mark success
            sqlx::query(
                "UPDATE upload_history SET status = 'completed', operations_count = $1 WHERE id = $2",
            )
            .bind(count)
            .bind(upload_id)
            .execute(&state.pool)
            .await
            .map_err(internal_err)?;

            Ok(Json(UploadResponse {
                upload_id,
                operations_count: count,
                status: "completed".to_string(),
            }))
        }
        Err(e) => {
            // Mark failure
            let _ = sqlx::query(
                "UPDATE upload_history SET status = 'failed', error_message = $1 WHERE id = $2",
            )
            .bind(e.to_string())
            .bind(upload_id)
            .execute(&state.pool)
            .await;

            Err((StatusCode::BAD_REQUEST, e.to_string()))
        }
    }
}

async fn extract_file(multipart: &mut Multipart) -> Result<(String, Vec<u8>), (StatusCode, String)> {
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("multipart error: {e}")))?
    {
        let name = field.name().unwrap_or("").to_string();
        if name == "file" {
            let filename = field
                .file_name()
                .unwrap_or("upload.zip")
                .to_string();
            let bytes = field
                .bytes()
                .await
                .map_err(|e| (StatusCode::BAD_REQUEST, format!("read error: {e}")))?;
            return Ok((filename, bytes.to_vec()));
        }
    }
    Err((StatusCode::BAD_REQUEST, "no 'file' field found".to_string()))
}

async fn process_zip(
    state: &AppState,
    tenant_id: Uuid,
    upload_id: Uuid,
    filename: &str,
    zip_bytes: &[u8],
) -> Result<i32, anyhow::Error> {
    // 1. Save original ZIP to R2
    let zip_key = format!("{}/uploads/{}/{}", tenant_id, upload_id, filename);
    state
        .storage
        .upload(&zip_key, zip_bytes, "application/zip")
        .await
        .map_err(|e| anyhow::anyhow!("R2 upload failed: {e}"))?;

    // Update upload_history with R2 key
    sqlx::query("UPDATE upload_history SET r2_zip_key = $1 WHERE id = $2")
        .bind(&zip_key)
        .bind(upload_id)
        .execute(&state.pool)
        .await?;

    // 2. Extract ZIP
    let files = csv_parser::extract_zip(zip_bytes)?;

    // 3. Find and parse KUDGURI.csv
    let kudguri_file = files
        .iter()
        .find(|(name, _)| name.to_uppercase().contains("KUDGURI"))
        .ok_or_else(|| anyhow::anyhow!("KUDGURI.csv not found in ZIP"))?;

    let csv_text = csv_parser::decode_shift_jis(&kudguri_file.1);
    let rows = parse_kudguri(&csv_text)?;
    tracing::info!("KUDGURI parsed: {} rows (tenant={})", rows.len(), tenant_id);

    if rows.is_empty() {
        return Ok(0);
    }

    // 3b. Find and parse KUDGIVT.csv
    let kudgivt_file = files
        .iter()
        .find(|(name, _)| name.to_uppercase().contains("KUDGIVT"))
        .ok_or_else(|| anyhow::anyhow!("KUDGIVT.csv not found in ZIP"))?;

    let kudgivt_text = csv_parser::decode_shift_jis(&kudgivt_file.1);
    let kudgivt_rows = parse_kudgivt(&kudgivt_text)?;
    tracing::info!("KUDGIVT parsed: {} rows (tenant={})", kudgivt_rows.len(), tenant_id);

    // 4. Upsert masters and insert operations
    let mut operations_count = 0i32;
    for row in &rows {
        // Upsert office
        let office_id = upsert_office(state, tenant_id, &row.office_cd, &row.office_name).await?;
        // Upsert vehicle
        let vehicle_id =
            upsert_vehicle(state, tenant_id, &row.vehicle_cd, &row.vehicle_name).await?;
        // Upsert driver
        let driver_id =
            upsert_driver(state, tenant_id, &row.driver_cd, &row.driver_name).await?;

        let r2_key_prefix = format!("{}/unko/{}", tenant_id, row.unko_no);

        // Delete existing operation with same (tenant_id, unko_no, crew_role) for re-upload
        sqlx::query(
            "DELETE FROM operations WHERE tenant_id = $1 AND unko_no = $2 AND crew_role = $3",
        )
        .bind(tenant_id)
        .bind(&row.unko_no)
        .bind(row.crew_role)
        .execute(&state.pool)
        .await?;

        // Insert operation
        sqlx::query(
            r#"INSERT INTO operations (
                tenant_id, unko_no, crew_role, reading_date, operation_date,
                office_id, vehicle_id, driver_id,
                departure_at, return_at, garage_out_at, garage_in_at,
                meter_start, meter_end, total_distance,
                drive_time_general, drive_time_highway, drive_time_bypass,
                safety_score, economy_score, total_score,
                raw_data, r2_key_prefix
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8,
                $9, $10, $11, $12,
                $13, $14, $15,
                $16, $17, $18,
                $19, $20, $21,
                $22, $23
            )"#,
        )
        .bind(tenant_id)
        .bind(&row.unko_no)
        .bind(row.crew_role)
        .bind(row.reading_date)
        .bind(row.operation_date)
        .bind(office_id)
        .bind(vehicle_id)
        .bind(driver_id)
        .bind(row.departure_at)
        .bind(row.return_at)
        .bind(row.garage_out_at)
        .bind(row.garage_in_at)
        .bind(row.meter_start)
        .bind(row.meter_end)
        .bind(row.total_distance)
        .bind(row.drive_time_general)
        .bind(row.drive_time_highway)
        .bind(row.drive_time_bypass)
        .bind(row.safety_score)
        .bind(row.economy_score)
        .bind(row.total_score)
        .bind(&row.raw_data)
        .bind(&r2_key_prefix)
        .execute(&state.pool)
        .await?;

        operations_count += 1;
    }

    tracing::info!("DB upsert done: {} operations (tenant={})", operations_count, tenant_id);

    // 5. Calculate daily_work_hours using KUDGIVT events
    // フェリー時間はCSV分割時にR2のKUDGFRYから取得済み（アップロード時はまだ未保存）
    let ferry_minutes: std::collections::HashMap<String, i32> = std::collections::HashMap::new();
    calculate_daily_hours(state, tenant_id, &rows, &kudgivt_rows, &ferry_minutes, None).await?;
    tracing::info!("calculate_daily_hours done (tenant={})", tenant_id);

    // 6. CSV split (inline)
    if let Err(e) = split_csv_from_r2(state, upload_id).await {
        tracing::warn!("CSV split failed (will not block): {e}");
    }

    Ok(operations_count)
}

async fn upsert_office(
    state: &AppState,
    tenant_id: Uuid,
    office_cd: &str,
    office_name: &str,
) -> Result<Option<Uuid>, anyhow::Error> {
    if office_cd.is_empty() {
        return Ok(None);
    }
    let rec = sqlx::query_as::<_, (Uuid,)>(
        r#"INSERT INTO offices (tenant_id, office_cd, office_name)
           VALUES ($1, $2, $3)
           ON CONFLICT (tenant_id, office_cd) DO UPDATE SET office_name = EXCLUDED.office_name
           RETURNING id"#,
    )
    .bind(tenant_id)
    .bind(office_cd)
    .bind(office_name)
    .fetch_one(&state.pool)
    .await?;
    Ok(Some(rec.0))
}

async fn upsert_vehicle(
    state: &AppState,
    tenant_id: Uuid,
    vehicle_cd: &str,
    vehicle_name: &str,
) -> Result<Option<Uuid>, anyhow::Error> {
    if vehicle_cd.is_empty() {
        return Ok(None);
    }
    let rec = sqlx::query_as::<_, (Uuid,)>(
        r#"INSERT INTO vehicles (tenant_id, vehicle_cd, vehicle_name)
           VALUES ($1, $2, $3)
           ON CONFLICT (tenant_id, vehicle_cd) DO UPDATE SET vehicle_name = EXCLUDED.vehicle_name
           RETURNING id"#,
    )
    .bind(tenant_id)
    .bind(vehicle_cd)
    .bind(vehicle_name)
    .fetch_one(&state.pool)
    .await?;
    Ok(Some(rec.0))
}

async fn upsert_driver(
    state: &AppState,
    tenant_id: Uuid,
    driver_cd: &str,
    driver_name: &str,
) -> Result<Option<Uuid>, anyhow::Error> {
    if driver_cd.is_empty() {
        return Ok(None);
    }
    let rec = sqlx::query_as::<_, (Uuid,)>(
        r#"INSERT INTO drivers (tenant_id, driver_cd, driver_name)
           VALUES ($1, $2, $3)
           ON CONFLICT (tenant_id, driver_cd) DO UPDATE SET driver_name = EXCLUDED.driver_name
           RETURNING id"#,
    )
    .bind(tenant_id)
    .bind(driver_cd)
    .bind(driver_name)
    .fetch_one(&state.pool)
    .await?;
    Ok(Some(rec.0))
}

/// 運行を「ワークデイ」にグルーピングする。
/// 休息基準（原則540分以上）を満たした場合、次の拘束開始を新規始業とする。
/// 満たさない場合は前の始業の日に帰属し続ける。24時間が最大。
/// Returns: unko_no → work_date のマッピング
fn group_operations_into_work_days(rows: &[KudguriRow]) -> std::collections::HashMap<String, chrono::NaiveDate> {
    use std::collections::HashMap;

    const REST_THRESHOLD_MINUTES: i64 = 540; // 原則休息基準
    const MAX_WORK_DAY_MINUTES: i64 = 1440; // 24時間

    let mut unko_work_date: HashMap<String, chrono::NaiveDate> = HashMap::new();

    // ドライバーごとにグルーピング
    let mut driver_rows: HashMap<String, Vec<&KudguriRow>> = HashMap::new();
    for row in rows {
        if !row.driver_cd.is_empty() {
            driver_rows.entry(row.driver_cd.clone()).or_default().push(row);
        }
    }

    for (_driver_cd, mut ops) in driver_rows {
        // departure_at でソート（Noneは末尾）
        ops.sort_by(|a, b| {
            let da = a.departure_at.or(a.garage_out_at);
            let db = b.departure_at.or(b.garage_out_at);
            da.cmp(&db)
        });

        let mut current_shigyo: Option<chrono::NaiveDateTime> = None; // 始業時刻
        let mut current_work_date: Option<chrono::NaiveDate> = None;
        let mut last_end: Option<chrono::NaiveDateTime> = None;

        for row in &ops {
            let dep = match row.departure_at.or(row.garage_out_at) {
                Some(d) => d,
                None => {
                    // departure_at がない場合は operation_date / reading_date で帰属
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
                    // 休息基準を満たした → 新規始業
                    new_day = true;
                } else if since_shigyo_minutes >= MAX_WORK_DAY_MINUTES {
                    // 24時間超過 → 強制日締め
                    new_day = true;
                }
            } else {
                // 最初の運行
                new_day = true;
            }

            if new_day {
                current_shigyo = Some(dep);
                current_work_date = Some(dep.date());
            }

            unko_work_date.insert(row.unko_no.clone(), current_work_date.unwrap());

            // last_end を更新（より遅い終了時刻を保持）
            last_end = Some(match last_end {
                Some(prev) if ret > prev => ret,
                Some(prev) => prev,
                None => ret,
            });
        }
    }

    unko_work_date
}

/// R2のKUDGFRYからフェリー乗船時間(分)を取得
/// Returns: unko_no → ferry_minutes のマッピング
async fn load_ferry_minutes(
    state: &AppState,
    tenant_id: Uuid,
    rows: &[KudguriRow],
) -> std::collections::HashMap<String, i32> {
    use std::collections::HashMap;

    let mut ferry_map: HashMap<String, i32> = HashMap::new();

    let futures: Vec<_> = rows
        .iter()
        .map(|row| {
            let r2_key = format!("{}/unko/{}/KUDGFRY.csv", tenant_id, row.unko_no);
            let storage = state.storage.clone();
            let unko_no = row.unko_no.clone();
            async move { (unko_no, storage.download(&r2_key).await) }
        })
        .collect();

    let results = futures::future::join_all(futures).await;

    for (unko_no, result) in results {
        if let Ok(bytes) = result {
            // KUDGFRY.csv: col 10=開始日時, col 11=終了日時
            // フェリー時間 = 終了 - 開始（分）
            let text = crate::csv_parser::decode_shift_jis(&bytes);
            let mut total_ferry = 0i32;
            for line in text.lines().skip(1) {
                let cols: Vec<&str> = line.split(',').collect();
                if cols.len() > 11 {
                    if let (Some(start), Some(end)) = (
                        chrono::NaiveDateTime::parse_from_str(cols[10].trim(), "%Y/%m/%d %H:%M:%S").ok()
                            .or_else(|| chrono::NaiveDateTime::parse_from_str(cols[10].trim(), "%Y/%m/%d %k:%M:%S").ok()),
                        chrono::NaiveDateTime::parse_from_str(cols[11].trim(), "%Y/%m/%d %H:%M:%S").ok()
                            .or_else(|| chrono::NaiveDateTime::parse_from_str(cols[11].trim(), "%Y/%m/%d %k:%M:%S").ok()),
                    ) {
                        let mins = (end - start).num_minutes() as i32;
                        if mins > 0 {
                            total_ferry += mins;
                            tracing::debug!("Ferry {}: {}min ({} → {})", unko_no, mins, start, end);
                        }
                    }
                }
            }
            if total_ferry > 0 {
                ferry_map.insert(unko_no, total_ferry);
            }
        }
    }

    tracing::info!("Ferry minutes loaded: {} operations with ferry", ferry_map.len());
    ferry_map
}

async fn calculate_daily_hours(
    state: &AppState,
    tenant_id: Uuid,
    rows: &[KudguriRow],
    kudgivt_rows: &[KudgivtRow],
    ferry_minutes: &std::collections::HashMap<String, i32>,
    progress_tx: Option<tokio::sync::mpsc::Sender<String>>,
) -> Result<(), anyhow::Error> {
    use std::collections::HashMap;

    // 0. 始業ベースのワークデイグルーピング（unko_no → work_date）
    let unko_work_date = group_operations_into_work_days(rows);

    // 1. Load or initialize event classifications
    let classifications = load_or_init_classifications(state, tenant_id, kudgivt_rows).await?;

    // 2. Group KUDGIVT rows by unko_no
    let mut kudgivt_by_unko: HashMap<String, Vec<&KudgivtRow>> = HashMap::new();
    for row in kudgivt_rows {
        kudgivt_by_unko
            .entry(row.unko_no.clone())
            .or_default()
            .push(row);
    }

    // 2.5. 302休息イベントを始業ベースのワークデイで集計
    let mut rest_event_map: HashMap<(String, chrono::NaiveDate), i32> = HashMap::new();
    for row in kudgivt_rows {
        if classifications.get(&row.event_cd) == Some(&EventClass::RestSplit) {
            let dur = row.duration_minutes.unwrap_or(0);
            if dur <= 0 { continue; }
            // unko_no からワークデイを取得（始業ベース帰属）
            let work_date = unko_work_date.get(&row.unko_no)
                .copied()
                .unwrap_or(row.start_at.date());
            *rest_event_map
                .entry((row.driver_cd.clone(), work_date))
                .or_insert(0) += dur;
        }
    }

    // 3. Aggregate per (driver_cd, date)
    struct DayAgg {
        driver_id: Option<Uuid>,
        total_work_minutes: i32,
        total_labor_minutes: i32,
        late_night_minutes: i32,
        drive_minutes: i32,
        cargo_minutes: i32,
        total_distance: f64,
        operation_count: i32,
        unko_nos: Vec<String>,
        segments: Vec<SegmentRecord>,
        rest_event_minutes: i32,
        // 24時間窓ベースの重複時間
        overlap_drive_minutes: i32,
        overlap_cargo_minutes: i32,
        overlap_break_minutes: i32,
        overlap_restraint_minutes: i32,
        // 時間外深夜（overlap統合時の深夜分）
        ot_late_night_minutes: i32,
    }

    #[derive(Clone)]
    struct SegmentRecord {
        unko_no: String,
        segment_index: i32,
        start_at: chrono::NaiveDateTime,
        end_at: chrono::NaiveDateTime,
        work_minutes: i32,
        labor_minutes: i32,
        late_night_minutes: i32,
        drive_minutes: i32,
        cargo_minutes: i32,
    }

    let mut day_map: HashMap<(String, chrono::NaiveDate), DayAgg> = HashMap::new();

    // unko_no → 出発日マップ（日跨ぎ運行を出発日に帰属させるため）
    let mut unko_departure_date: HashMap<String, chrono::NaiveDate> = HashMap::new();
    for row in rows {
        if let Some(dep) = row.departure_at {
            unko_departure_date.insert(row.unko_no.clone(), dep.date());
        }
    }

    for row in rows {
        let driver_id = if !row.driver_cd.is_empty() {
            let rec = sqlx::query_as::<_, (Uuid,)>(
                "SELECT id FROM drivers WHERE tenant_id = $1 AND driver_cd = $2",
            )
            .bind(tenant_id)
            .bind(&row.driver_cd)
            .fetch_optional(&state.pool)
            .await?;
            rec.map(|r| r.0)
        } else {
            None
        };

        let total_distance = row.total_distance.unwrap_or(0.0);

        match (row.departure_at, row.return_at) {
            (Some(dep), Some(ret)) if ret > dep => {
                // KUDGIVTイベントで休息分割
                let events = kudgivt_by_unko.get(&row.unko_no);
                let event_slice: Vec<&KudgivtRow> = events
                    .map(|e| e.iter().copied().collect())
                    .unwrap_or_default();

                let segments = work_segments::split_by_rest(dep, ret, &event_slice, &classifications);
                let daily_segments = work_segments::split_segments_by_day(&segments);

                // 総拘束時間（走行距離按分用）
                let total_work_mins: i32 = daily_segments.iter().map(|s| s.work_minutes).sum();

                for ds in &daily_segments {
                    let ratio = if total_work_mins > 0 {
                        ds.work_minutes as f64 / total_work_mins as f64
                    } else {
                        0.0
                    };
                    let day_distance = total_distance * ratio;

                    // work_date: 各DailyWorkSegmentが属するWorkSegment(休息分割後)の開始日を使用
                    // → 日跨ぎ(00:00分割)は親セグメントの開始日に帰属
                    // → 休息で分割された場合は新しいセグメントの開始日に帰属
                    let work_date = segments.iter()
                        .find(|seg| ds.start >= seg.start && ds.start < seg.end)
                        .map(|seg| seg.start.date())
                        .unwrap_or(ds.date);
                    let entry = day_map
                        .entry((row.driver_cd.clone(), work_date))
                        .or_insert(DayAgg {
                            driver_id,
                            total_work_minutes: 0,
                            total_labor_minutes: 0,
                            late_night_minutes: 0,
                            drive_minutes: 0,
                            cargo_minutes: 0,
                            total_distance: 0.0,
                            operation_count: 0,
                            unko_nos: Vec::new(),
                            segments: Vec::new(),
                            rest_event_minutes: 0,
                            overlap_drive_minutes: 0,
                            overlap_cargo_minutes: 0,
                            overlap_break_minutes: 0,
                            overlap_restraint_minutes: 0,
                            ot_late_night_minutes: 0,
                        });

                    entry.total_work_minutes += ds.work_minutes;
                    entry.total_labor_minutes += ds.labor_minutes;
                    entry.late_night_minutes += ds.late_night_minutes;
                    entry.drive_minutes += ds.drive_minutes;
                    entry.cargo_minutes += ds.cargo_minutes;
                    entry.total_distance += day_distance;
                    if !entry.unko_nos.contains(&row.unko_no) {
                        entry.unko_nos.push(row.unko_no.clone());
                        entry.operation_count += 1;
                    }
                    if entry.driver_id.is_none() {
                        entry.driver_id = driver_id;
                    }

                    let seg_idx = entry
                        .segments
                        .iter()
                        .filter(|s| s.unko_no == row.unko_no)
                        .count() as i32;

                    entry.segments.push(SegmentRecord {
                        unko_no: row.unko_no.clone(),
                        segment_index: seg_idx,
                        start_at: ds.start,
                        end_at: ds.end,
                        work_minutes: ds.work_minutes,
                        labor_minutes: ds.labor_minutes,
                        late_night_minutes: ds.late_night_minutes,
                        drive_minutes: ds.drive_minutes,
                        cargo_minutes: ds.cargo_minutes,
                    });
                }
            }
            _ => {
                // 出社・退社がない場合は運行日（or 読取日）に集約
                let work_date = row.operation_date.unwrap_or(row.reading_date);
                let total_drive_mins = row.drive_time_general.unwrap_or(0)
                    + row.drive_time_highway.unwrap_or(0)
                    + row.drive_time_bypass.unwrap_or(0);

                let entry = day_map
                    .entry((row.driver_cd.clone(), work_date))
                    .or_insert(DayAgg {
                        driver_id,
                        total_work_minutes: 0,
                        total_labor_minutes: 0,
                        late_night_minutes: 0,
                        drive_minutes: 0,
                        cargo_minutes: 0,
                        total_distance: 0.0,
                        operation_count: 0,
                        unko_nos: Vec::new(),
                        segments: Vec::new(),
                        rest_event_minutes: 0,
                        overlap_drive_minutes: 0,
                        overlap_cargo_minutes: 0,
                        overlap_break_minutes: 0,
                        overlap_restraint_minutes: 0,
                        ot_late_night_minutes: 0,
                    });

                entry.total_work_minutes += total_drive_mins;
                entry.total_labor_minutes += total_drive_mins;
                entry.total_distance += total_distance;
                entry.operation_count += 1;
                entry.unko_nos.push(row.unko_no.clone());
                if entry.driver_id.is_none() {
                    entry.driver_id = driver_id;
                }
            }
        }
    }

    // 3.5. rest_event_mapをday_mapに反映
    for ((driver_cd, date), rest_mins) in &rest_event_map {
        if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), *date)) {
            agg.rest_event_minutes = *rest_mins;
        }
    }

    // 3.55. drive/cargo をKUDGIVTイベントから日別に直接集計（カレンダー日付ベース）
    //       total_work_minutes はセグメントから秒単位で四捨五入
    {
        let mut driver_unko_map: HashMap<String, Vec<String>> = HashMap::new();
        for ((driver_cd, _), agg) in &day_map {
            let entry = driver_unko_map.entry(driver_cd.clone()).or_default();
            for u in &agg.unko_nos {
                if !entry.contains(u) {
                    entry.push(u.clone());
                }
            }
        }

        for (driver_cd, unko_nos) in &driver_unko_map {
            let mut day_drive: HashMap<chrono::NaiveDate, i32> = HashMap::new();
            let mut day_cargo: HashMap<chrono::NaiveDate, i32> = HashMap::new();
            let mut day_break: HashMap<chrono::NaiveDate, i32> = HashMap::new();
            let mut day_late_night: HashMap<chrono::NaiveDate, i32> = HashMap::new();

            for unko_no in unko_nos {
                if let Some(events) = kudgivt_by_unko.get(unko_no) {
                    for evt in events {
                        let dur = evt.duration_minutes.unwrap_or(0);
                        if dur <= 0 { continue; }
                        // イベントの帰属日: unko_noを含むday_mapエントリのうち
                        // イベント日以前で最も近いwork_dateを使用
                        // (多日運行では同一unko_noが複数日に存在するため)
                        let cal_date = evt.start_at.date();
                        let event_date = day_map.iter()
                            .filter(|((dc, d), agg)| dc == driver_cd && *d <= cal_date && agg.unko_nos.contains(unko_no))
                            .map(|((_, d), _)| *d)
                            .max()
                            .unwrap_or(cal_date);
                        let cls = classifications.get(&evt.event_cd);
                        match cls {
                            Some(EventClass::Drive) => {
                                *day_drive.entry(event_date).or_insert(0) += dur;
                            }
                            Some(EventClass::Cargo) => {
                                *day_cargo.entry(event_date).or_insert(0) += dur;
                            }
                            Some(EventClass::Break) => {
                                *day_break.entry(event_date).or_insert(0) += dur;
                            }
                            _ => {}
                        }
                        // 深夜時間: Drive/Cargo イベントの実時間のみから計算
                        match cls {
                            Some(EventClass::Drive) | Some(EventClass::Cargo) => {
                                let evt_end = evt.start_at + chrono::Duration::minutes(dur as i64);
                                let night = crate::csv_parser::work_segments::calc_late_night_mins(evt.start_at, evt_end);
                                if night > 0 {
                                    *day_late_night.entry(event_date).or_insert(0) += night;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }

            for (date, drive) in &day_drive {
                if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), *date)) {
                    agg.drive_minutes = *drive;
                }
            }
            for (date, cargo) in &day_cargo {
                if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), *date)) {
                    agg.cargo_minutes = *cargo;
                }
            }
            // total_work_minutes をイベント合計(drive+cargo+break)で上書き
            // イベントのduration_minutesは整数分で正確（セグメントwall-clockは秒切り捨てでズレる）
            // 204(その他)→cargo, 205(待機)→breakに分類修正済み
            for ((dc, date), agg) in day_map.iter_mut() {
                if dc != driver_cd { continue; }
                let d = day_drive.get(date).copied().unwrap_or(0);
                let c = day_cargo.get(date).copied().unwrap_or(0);
                let b = day_break.get(date).copied().unwrap_or(0);
                let event_total = d + c + b;
                if event_total > 0 {
                    agg.total_work_minutes = event_total;
                }
            }
            // 深夜時間をイベントベース(Drive/Cargo during 22:00-05:00)で上書き
            for (date, night) in &day_late_night {
                if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), *date)) {
                    agg.late_night_minutes = *night;
                }
            }
            // 深夜イベントがない日は0にリセット
            for ((dc, _date), agg) in day_map.iter_mut() {
                if dc == driver_cd && !day_late_night.contains_key(_date) {
                    agg.late_night_minutes = 0;
                }
            }
            // ot_late_night = 始業+8h(所定労働)後に発生するDrive/Cargo深夜時間
            for (date, night) in &day_late_night {
                if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), *date)) {
                    let shigyo = agg.segments.iter().map(|s| s.start_at).min();
                    let ot_night = if let Some(start) = shigyo {
                        let overtime_start = start + chrono::Duration::minutes(480);
                        // 深夜帯: 始業日の22:00と翌05:00
                        let night_start_22 = start.date().and_hms_opt(22, 0, 0).unwrap();
                        let night_end_05 = (start.date() + chrono::Duration::days(1))
                            .and_hms_opt(5, 0, 0).unwrap();
                        // 始業が22:00-05:00の場合、深夜帯の終了は始業当日の05:00または翌05:00
                        let effective_night_end = if start.hour() < 5 {
                            // 始業が0-5時 → 深夜帯終了は当日05:00
                            start.date().and_hms_opt(5, 0, 0).unwrap()
                        } else {
                            night_end_05
                        };
                        if overtime_start >= effective_night_end {
                            // 始業+8hが深夜帯終了以降 → 深夜帯は全て所定労働内
                            0
                        } else if overtime_start <= night_start_22 {
                            // 始業+8hが22:00より前 → 深夜帯は全て時間外
                            *night
                        } else {
                            // 部分的: overtime_startが深夜帯内 → 近似値
                            *night
                        }
                    } else {
                        0
                    };
                    agg.ot_late_night_minutes = ot_night;
                }
            }
        }

        // total_work_minutes: 拘束時間小計 = セグメント壁時計合計 - フェリー乗船時間
        // セグメントは休息(302)で分割済みなので休息時間は既に除外されている
        // フェリー時間はKUDGFRYから取得し、運行単位で控除する
        for ((_driver_cd, _date), agg) in day_map.iter_mut() {
            let mut ferry_deduction = 0i32;
            for unko in &agg.unko_nos {
                if let Some(&fm) = ferry_minutes.get(unko) {
                    ferry_deduction += fm;
                }
            }
            if ferry_deduction > 0 {
                agg.total_work_minutes = (agg.total_work_minutes - ferry_deduction).max(0);
            }
        }
    }

    // 3.6. 24時間窓ベースの重複時間を計算（KUDGIVTイベント直接集計）
    {
        use std::collections::BTreeMap;

        // 秒を切り捨てて分精度に揃える（Excel互換）
        fn trunc_min(dt: chrono::NaiveDateTime) -> chrono::NaiveDateTime {
            dt.with_second(0).unwrap_or(dt)
        }

        // ドライバーCD → 日付順の (始業, 終業, unko_nos)
        struct DayInfo {
            start: chrono::NaiveDateTime,
            end: chrono::NaiveDateTime,
            unko_nos: Vec<String>,
        }

        let mut driver_days: HashMap<String, BTreeMap<chrono::NaiveDate, DayInfo>> = HashMap::new();
        for ((driver_cd, date), agg) in &day_map {
            if agg.segments.is_empty() { continue; }
            let start = trunc_min(agg.segments.iter().map(|s| s.start_at).min().unwrap());
            let end = trunc_min(agg.segments.iter().map(|s| s.end_at).max().unwrap());
            driver_days.entry(driver_cd.clone()).or_default()
                .insert(*date, DayInfo { start, end, unko_nos: agg.unko_nos.clone() });
        }

        for (driver_cd, dates_map) in &driver_days {
            let dates: Vec<chrono::NaiveDate> = dates_map.keys().copied().collect();
            let mut effective_start: Option<chrono::NaiveDateTime> = None;
            let mut prev_end: Option<chrono::NaiveDateTime> = None;
            // 前日から繰り越す重複分の控除（リセットなし時にメイン統合するため）
            let mut next_day_deduction: Option<(i32, i32, i32, i32)> = None; // (drive, cargo, restraint, late_night)

            for (idx, &date) in dates.iter().enumerate() {
                let info = &dates_map[&date];

                // effective_start の判定（8時間ルール）
                let reset = match prev_end {
                    Some(pe) => (info.start - pe).num_minutes() >= 480,
                    None => true,
                };
                if reset {
                    effective_start = Some(info.start);
                    next_day_deduction = None;
                } else {
                    // リセットなし: 前日の24h窓終了時点から新しい24h窓を開始
                    effective_start = Some(effective_start.unwrap() + chrono::Duration::hours(24));
                }

                // 前日からの控除を適用（リセットなし時、前日のoverlap分を当日メインから減算）
                if let Some((ded_drive, ded_cargo, ded_restraint, ded_night)) = next_day_deduction.take() {
                    if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), date)) {
                        agg.drive_minutes = (agg.drive_minutes - ded_drive).max(0);
                        agg.cargo_minutes = (agg.cargo_minutes - ded_cargo).max(0);
                        agg.total_work_minutes = (agg.total_work_minutes - ded_restraint).max(0);
                        agg.late_night_minutes = (agg.late_night_minutes - ded_night).max(0);
                    }
                }

                let window_end = effective_start.unwrap() + chrono::Duration::hours(24);

                // 翌稼働日のKUDGIVTイベントで window_end 以前のものを直接集計
                if idx + 1 < dates.len() {
                    let next_info = &dates_map[&dates[idx + 1]];

                    // 翌日の全unko_noのイベントを取得
                    let mut ol_drive = 0i32;
                    let mut ol_cargo = 0i32;
                    let mut ol_restraint = 0i32;

                    for unko_no in &next_info.unko_nos {
                        if let Some(events) = kudgivt_by_unko.get(unko_no) {
                            for evt in events {
                                let cls = classifications.get(&evt.event_cd);
                                let dur = evt.duration_minutes.unwrap_or(0);
                                if dur <= 0 { continue; }

                                // イベント開始も分精度に揃える（Excel互換）
                                let evt_start = trunc_min(evt.start_at);

                                // イベントが窓内かチェック
                                if evt_start >= window_end { continue; }
                                let evt_end = evt_start + chrono::Duration::minutes(dur as i64);

                                // 当日の終業より前のイベントはスキップ（当日に属する）
                                if evt_end <= info.end { continue; }
                                if evt_start < info.end { continue; }

                                // 窓内に収まる分だけカウント
                                // イベント起点はセグメント開始(next_info.start)以降に制限
                                let overlap_start = evt_start.max(next_info.start);
                                let effective_end = evt_end.min(window_end);
                                if effective_end <= overlap_start { continue; }
                                let mins = (effective_end - overlap_start).num_minutes() as i32;
                                if mins <= 0 { continue; }

                                // イベント全体が窓内ならそのまま、部分的なら按分
                                let actual_dur = if mins >= dur {
                                    dur
                                } else {
                                    mins
                                };

                                match cls {
                                    Some(EventClass::Drive) => ol_drive += actual_dur,
                                    Some(EventClass::Cargo) => ol_cargo += actual_dur,
                                    _ => {}
                                }
                            }
                        }
                    }

                    // 重複拘束時間 = 翌日始業 ～ 窓内最終セグメント終了（分精度）
                    // セグメント間の休息ギャップが窓内にある場合、最終セグメント終了で打ち切る
                    if next_info.start < window_end {
                        let next_date = dates[idx + 1];
                        let restraint_end = day_map
                            .get(&(driver_cd.clone(), next_date))
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

                    let ol_break = (ol_restraint - ol_drive - ol_cargo).max(0);

                    // 翌日がリセットなし（8h未満の休息）→ 重複を当日メインに統合
                    let next_gap = (next_info.start - info.end).num_minutes();
                    let next_resets = next_gap >= 480;

                    if !next_resets && ol_restraint > 0 {
                        // 重複期間の深夜時間を計算（翌日から控除するため）
                        let ol_late_night = {
                            use crate::csv_parser::work_segments::calc_late_night_mins;
                            calc_late_night_mins(next_info.start, window_end)
                        };
                        // 当日メインに統合（overlapは0にする）
                        // 深夜は重複列で扱うため当日メインには加算しない
                        if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), date)) {
                            agg.drive_minutes += ol_drive;
                            agg.cargo_minutes += ol_cargo;
                            agg.total_work_minutes += ol_restraint;
                            agg.ot_late_night_minutes = ol_late_night;
                        }
                        // 翌日のメインから控除する分を記録（深夜も控除）
                        next_day_deduction = Some((ol_drive, ol_cargo, ol_restraint, ol_late_night));
                    } else {
                        // 通常: 重複として別表示
                        if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), date)) {
                            agg.overlap_drive_minutes = ol_drive;
                            agg.overlap_cargo_minutes = ol_cargo;
                            agg.overlap_break_minutes = ol_break;
                            agg.overlap_restraint_minutes = ol_restraint;
                        }
                    }
                }

                prev_end = Some(info.end);
            }
        }
    }

    // 4. Persist to DB
    // 日跨ぎ修正で帰属日が変わると古い日のデータが残るため、
    // 対象ドライバー×unko_noの既存データを一括削除してから再挿入する
    {
        // 全対象 unko_no を収集
        let mut all_unko_nos: Vec<String> = Vec::new();
        let mut driver_ids_seen: std::collections::HashSet<Uuid> = std::collections::HashSet::new();
        for ((_dc, _wd), agg) in &day_map {
            if let Some(did) = agg.driver_id {
                driver_ids_seen.insert(did);
            }
            for u in &agg.unko_nos {
                if !all_unko_nos.contains(u) {
                    all_unko_nos.push(u.clone());
                }
            }
        }
        // unko_noベースで古いセグメント・daily_work_hours を削除
        for did in &driver_ids_seen {
            for unko in &all_unko_nos {
                sqlx::query(
                    "DELETE FROM daily_work_segments WHERE tenant_id = $1 AND driver_id = $2 AND unko_no = $3",
                )
                .bind(tenant_id)
                .bind(did)
                .bind(unko)
                .execute(&state.pool)
                .await?;
            }
            // unko_nosカラム（配列）に含まれるエントリも削除
            sqlx::query(
                "DELETE FROM daily_work_hours WHERE tenant_id = $1 AND driver_id = $2 AND unko_nos && $3",
            )
            .bind(tenant_id)
            .bind(did)
            .bind(&all_unko_nos)
            .execute(&state.pool)
            .await?;
        }
    }

    let day_entries: Vec<_> = day_map.iter().collect();
    let save_total = day_entries.len();
    for (i, ((_driver_cd, work_date), agg)) in day_entries.into_iter().enumerate() {
        let Some(driver_id) = agg.driver_id else {
            continue;
        };

        let rest_minutes = agg.rest_event_minutes;

        // Delete existing for re-upload
        sqlx::query(
            "DELETE FROM daily_work_hours WHERE tenant_id = $1 AND driver_id = $2 AND work_date = $3",
        )
        .bind(tenant_id)
        .bind(driver_id)
        .bind(work_date)
        .execute(&state.pool)
        .await?;

        sqlx::query(
            r#"INSERT INTO daily_work_hours (
                tenant_id, driver_id, work_date,
                total_work_minutes, total_drive_minutes, total_rest_minutes,
                late_night_minutes, drive_minutes, cargo_minutes,
                total_distance, operation_count, unko_nos,
                overlap_drive_minutes, overlap_cargo_minutes,
                overlap_break_minutes, overlap_restraint_minutes,
                ot_late_night_minutes
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)"#,
        )
        .bind(tenant_id)
        .bind(driver_id)
        .bind(work_date)
        .bind(agg.total_work_minutes)
        .bind(agg.total_labor_minutes)
        .bind(rest_minutes)
        .bind(agg.late_night_minutes)
        .bind(agg.drive_minutes)
        .bind(agg.cargo_minutes)
        .bind(agg.total_distance)
        .bind(agg.operation_count)
        .bind(&agg.unko_nos)
        .bind(agg.overlap_drive_minutes)
        .bind(agg.overlap_cargo_minutes)
        .bind(agg.overlap_break_minutes)
        .bind(agg.overlap_restraint_minutes)
        .bind(agg.ot_late_night_minutes)
        .execute(&state.pool)
        .await?;

        // Delete and re-insert segments
        sqlx::query(
            "DELETE FROM daily_work_segments WHERE tenant_id = $1 AND driver_id = $2 AND work_date = $3",
        )
        .bind(tenant_id)
        .bind(driver_id)
        .bind(work_date)
        .execute(&state.pool)
        .await?;

        for seg in &agg.segments {
            sqlx::query(
                r#"INSERT INTO daily_work_segments (
                    tenant_id, driver_id, work_date, unko_no, segment_index,
                    start_at, end_at, work_minutes, labor_minutes, late_night_minutes,
                    drive_minutes, cargo_minutes
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"#,
            )
            .bind(tenant_id)
            .bind(driver_id)
            .bind(work_date)
            .bind(&seg.unko_no)
            .bind(seg.segment_index)
            .bind(seg.start_at)
            .bind(seg.end_at)
            .bind(seg.work_minutes)
            .bind(seg.labor_minutes)
            .bind(seg.late_night_minutes)
            .bind(seg.drive_minutes)
            .bind(seg.cargo_minutes)
            .execute(&state.pool)
            .await?;
        }

        if let Some(ref ptx) = progress_tx {
            if (i + 1) % 20 == 0 || i + 1 == save_total {
                let msg = serde_json::json!({
                    "event": "progress",
                    "current": i + 1,
                    "total": save_total,
                    "step": "save"
                });
                let _ = ptx.send(format!("data: {}\n\n", msg)).await;
            }
        }
    }

    Ok(())
}

/// R2のZIPからKUDGIVTを取得（テナント・月の全ZIPを走査）
async fn load_kudgivt_from_zips(
    state: &AppState,
    tenant_id: Uuid,
    month_start: chrono::NaiveDate,
    month_end: chrono::NaiveDate,
) -> Result<Vec<KudgivtRow>, anyhow::Error> {
    // 該当月のupload_historyからZIPキーを取得
    let zip_keys: Vec<String> = sqlx::query_scalar(
        r#"SELECT DISTINCT r2_zip_key FROM upload_history
           WHERE tenant_id = $1 AND status = 'completed'
             AND created_at >= ($2::date - interval '60 days')
           ORDER BY r2_zip_key"#,
    )
    .bind(tenant_id)
    .bind(month_start)
    .fetch_all(&state.pool)
    .await?;

    let mut all_kudgivt = Vec::new();
    let mut seen_zips = std::collections::HashSet::new();

    for zip_key in &zip_keys {
        if seen_zips.contains(zip_key) { continue; }
        seen_zips.insert(zip_key.clone());

        match state.storage.download(zip_key).await {
            Ok(zip_bytes) => {
                match csv_parser::extract_zip(&zip_bytes) {
                    Ok(files) => {
                        if let Some((_, bytes)) = files.iter().find(|(name, _)| name.to_uppercase().contains("KUDGIVT")) {
                            let text = csv_parser::decode_shift_jis(bytes);
                            match parse_kudgivt(&text) {
                                Ok(rows) => {
                                    tracing::info!("KUDGIVT from ZIP {}: {} rows", zip_key, rows.len());
                                    all_kudgivt.extend(rows);
                                }
                                Err(e) => tracing::warn!("KUDGIVT parse error in {}: {e}", zip_key),
                            }
                        }
                    }
                    Err(e) => tracing::warn!("ZIP extract error {}: {e}", zip_key),
                }
            }
            Err(e) => tracing::warn!("ZIP download error {}: {e}", zip_key),
        }
    }

    tracing::info!("Total KUDGIVT from ZIPs: {} rows", all_kudgivt.len());
    Ok(all_kudgivt)
}

/// イベント分類をDBから取得、なければデフォルトで初期化
async fn load_or_init_classifications(
    state: &AppState,
    tenant_id: Uuid,
    kudgivt_rows: &[KudgivtRow],
) -> Result<std::collections::HashMap<String, EventClass>, anyhow::Error> {
    use std::collections::HashMap;

    // DBから既存の分類を取得
    let existing: Vec<(String, String)> = sqlx::query_as(
        "SELECT event_cd, classification FROM event_classifications WHERE tenant_id = $1",
    )
    .bind(tenant_id)
    .fetch_all(&state.pool)
    .await?;

    let mut map: HashMap<String, EventClass> = HashMap::new();
    for (cd, cls) in &existing {
        let ec = match cls.as_str() {
            "drive" => EventClass::Drive,
            "cargo" => EventClass::Cargo,
            "work" => EventClass::Drive, // legacy fallback
            "rest_split" => EventClass::RestSplit,
            "break" => EventClass::Break,
            _ => EventClass::Ignore,
        };
        map.insert(cd.clone(), ec);
    }

    // 未登録のイベントをKUDGIVTから検出してデフォルト分類で登録
    let mut seen: std::collections::HashSet<String> = map.keys().cloned().collect();
    for row in kudgivt_rows {
        if seen.contains(&row.event_cd) {
            continue;
        }
        seen.insert(row.event_cd.clone());

        let (cls_str, ec) = default_classification(&row.event_cd);
        map.insert(row.event_cd.clone(), ec);

        let _ = sqlx::query(
            r#"INSERT INTO event_classifications (tenant_id, event_cd, event_name, classification)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (tenant_id, event_cd) DO NOTHING"#,
        )
        .bind(tenant_id)
        .bind(&row.event_cd)
        .bind(&row.event_name)
        .bind(cls_str)
        .execute(&state.pool)
        .await;
    }

    Ok(map)
}

fn default_classification(event_cd: &str) -> (&'static str, EventClass) {
    match event_cd {
        "110" => ("drive", EventClass::Drive),          // IG-Moving(運転)
        "202" => ("cargo", EventClass::Cargo),          // 積み
        "203" => ("cargo", EventClass::Cargo),          // 降し
        "302" => ("rest_split", EventClass::RestSplit), // 休息
        "301" => ("break", EventClass::Break),          // 休憩
        _ => ("ignore", EventClass::Ignore),            // その他は無視
    }
}

/// 内部用アップロード（認証なし、tenant_id はフォームで指定）
async fn internal_upload_zip(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, (StatusCode, String)> {
    let mut tenant_id_str = None;
    let mut file_data = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("multipart error: {e}")))?
    {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "tenant_id" => {
                tenant_id_str = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| (StatusCode::BAD_REQUEST, format!("read tenant_id: {e}")))?,
                );
            }
            "file" => {
                let filename = field
                    .file_name()
                    .unwrap_or("upload.zip")
                    .to_string();
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|e| (StatusCode::BAD_REQUEST, format!("read file: {e}")))?;
                file_data = Some((filename, bytes.to_vec()));
            }
            _ => {}
        }
    }

    let tenant_id_str = tenant_id_str
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "missing tenant_id field".into()))?;
    let tenant_id = Uuid::parse_str(&tenant_id_str)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid tenant_id: {e}")))?;
    let (filename, zip_bytes) =
        file_data.ok_or_else(|| (StatusCode::BAD_REQUEST, "missing file field".into()))?;

    let upload_id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO upload_history (id, tenant_id, uploaded_by, filename, status)
           VALUES ($1, $2, $3, $4, 'processing')"#,
    )
    .bind(upload_id)
    .bind(tenant_id)
    .bind(None::<Uuid>) // internal: no user
    .bind(&filename)
    .execute(&state.pool)
    .await
    .map_err(internal_err)?;

    match process_zip(&state, tenant_id, upload_id, &filename, &zip_bytes).await {
        Ok(count) => {
            sqlx::query(
                "UPDATE upload_history SET status = 'completed', operations_count = $1 WHERE id = $2",
            )
            .bind(count)
            .bind(upload_id)
            .execute(&state.pool)
            .await
            .map_err(internal_err)?;

            Ok(Json(UploadResponse {
                upload_id,
                operations_count: count,
                status: "completed".to_string(),
            }))
        }
        Err(e) => {
            let _ = sqlx::query(
                "UPDATE upload_history SET status = 'failed', error_message = $1 WHERE id = $2",
            )
            .bind(e.to_string())
            .bind(upload_id)
            .execute(&state.pool)
            .await;

            Err((StatusCode::BAD_REQUEST, e.to_string()))
        }
    }
}

fn internal_err(e: impl std::fmt::Display) -> (StatusCode, String) {
    tracing::error!("internal error: {e}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal server error".to_string(),
    )
}

/// ZIP を R2 に保存のみ（処理なし）。アップロード失敗時の退避用。
async fn internal_store_zip(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, (StatusCode, String)> {
    let mut tenant_id_str = None;
    let mut file_data = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("multipart error: {e}")))?
    {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "tenant_id" => {
                tenant_id_str = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| (StatusCode::BAD_REQUEST, format!("read tenant_id: {e}")))?,
                );
            }
            "file" => {
                let filename = field
                    .file_name()
                    .unwrap_or("upload.zip")
                    .to_string();
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|e| (StatusCode::BAD_REQUEST, format!("read file: {e}")))?;
                file_data = Some((filename, bytes.to_vec()));
            }
            _ => {}
        }
    }

    let tenant_id_str = tenant_id_str
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "missing tenant_id field".into()))?;
    let tenant_id = Uuid::parse_str(&tenant_id_str)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid tenant_id: {e}")))?;
    let (filename, zip_bytes) =
        file_data.ok_or_else(|| (StatusCode::BAD_REQUEST, "missing file field".into()))?;

    let upload_id = Uuid::new_v4();

    // R2 に ZIP を保存 (pending/ プレフィックス → ライフサイクルルールで7日後に自動削除)
    let zip_key = format!("{}/pending/{}/{}", tenant_id, upload_id, filename);
    state
        .storage
        .upload(&zip_key, &zip_bytes, "application/zip")
        .await
        .map_err(internal_err)?;

    // upload_history に pending_retry で記録
    sqlx::query(
        r#"INSERT INTO upload_history (id, tenant_id, uploaded_by, filename, status, r2_zip_key)
           VALUES ($1, $2, $3, $4, 'pending_retry', $5)"#,
    )
    .bind(upload_id)
    .bind(tenant_id)
    .bind(None::<Uuid>)
    .bind(&filename)
    .bind(&zip_key)
    .execute(&state.pool)
    .await
    .map_err(internal_err)?;

    tracing::info!("ZIP stored for retry: upload_id={}, key={}", upload_id, zip_key);

    Ok(Json(UploadResponse {
        upload_id,
        operations_count: 0,
        status: "pending_retry".to_string(),
    }))
}

/// Cloud Tasks に CSV 分割タスクをエンキュー
async fn enqueue_csv_split(state: &AppState, upload_id: Uuid) -> Result<(), anyhow::Error> {
    let config = match &state.cloud_tasks {
        Some(c) => c,
        None => {
            tracing::info!("Cloud Tasks not configured, running CSV split inline");
            split_csv_from_r2(state, upload_id).await?;
            return Ok(());
        }
    };

    // GCP メタデータサーバーからアクセストークンを取得
    let token_url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
    let client = reqwest::Client::new();
    let token_resp = client
        .get(token_url)
        .header("Metadata-Flavor", "Google")
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    let access_token = token_resp["access_token"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("No access_token in metadata response"))?;

    let task_url = format!(
        "{}/internal/split-csv/{}",
        config.self_url, upload_id
    );

    let task_body = serde_json::json!({
        "task": {
            "httpRequest": {
                "httpMethod": "POST",
                "url": task_url,
                "oidcToken": {
                    "serviceAccountEmail": config.service_account_email,
                }
            }
        }
    });

    let api_url = format!(
        "https://cloudtasks.googleapis.com/v2/{}/tasks",
        config.queue_path
    );

    let resp = client
        .post(&api_url)
        .bearer_auth(access_token)
        .json(&task_body)
        .send()
        .await?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Cloud Tasks API error: {}", body);
    }

    tracing::info!("CSV split task enqueued for upload_id={}", upload_id);
    Ok(())
}

/// R2 から ZIP をダウンロードして CSV を unko_no 別に分割アップロード
async fn split_csv_from_r2(state: &AppState, upload_id: Uuid) -> Result<(), anyhow::Error> {
    let record = sqlx::query_as::<_, (Uuid, String)>(
        "SELECT tenant_id, r2_zip_key FROM upload_history WHERE id = $1",
    )
    .bind(upload_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| anyhow::anyhow!("upload {} not found", upload_id))?;

    let (tenant_id, r2_zip_key) = record;

    let zip_bytes = state.storage.download(&r2_zip_key).await
        .map_err(|e| anyhow::anyhow!("R2 download failed: {e}"))?;

    let files = csv_parser::extract_zip(&zip_bytes)?;

    let mut kudgivt_unko_nos: Vec<String> = Vec::new();

    // アップロード対象を事前に全て準備
    let mut upload_items: Vec<(String, Vec<u8>, bool)> = Vec::new(); // (key, content, is_kudgivt)
    for (name, bytes) in &files {
        if name.to_lowercase().ends_with(".csv") {
            let utf8_text = csv_parser::decode_shift_jis(bytes);
            let header = csv_parser::csv_header(&utf8_text);
            let grouped = csv_parser::group_csv_by_unko_no(&utf8_text);
            let is_kudgivt = name.to_uppercase().contains("KUDGIVT");

            for (unko_no, lines) in &grouped {
                let csv_name = name
                    .rsplit('/')
                    .next()
                    .unwrap_or(name)
                    .to_uppercase()
                    .replace(".CSV", ".csv");
                let key = format!("{}/unko/{}/{}", tenant_id, unko_no, csv_name);
                let mut content = String::new();
                if let Some(h) = header {
                    content.push_str(h);
                    content.push('\n');
                }
                for line in lines {
                    content.push_str(line);
                    content.push('\n');
                }
                upload_items.push((key, content.into_bytes(), is_kudgivt));

                if is_kudgivt {
                    kudgivt_unko_nos.push(unko_no.clone());
                }
            }
        }
    }

    // バッチ並列アップロード（20並列）
    let batch_size = 20;
    let mut csv_count = 0usize;
    for chunk in upload_items.chunks(batch_size) {
        let futures: Vec<_> = chunk.iter().map(|(key, content, _)| {
            let storage = state.storage.clone();
            let k = key.clone();
            let c = content.clone();
            async move { storage.upload(&k, &c, "text/csv").await }
        }).collect();
        let results = futures::future::join_all(futures).await;
        csv_count += results.len();
    }

    // has_kudgivt フラグを更新
    if !kudgivt_unko_nos.is_empty() {
        for chunk in kudgivt_unko_nos.chunks(100) {
            let placeholders: Vec<String> = chunk.iter().enumerate()
                .map(|(i, _)| format!("${}", i + 2))
                .collect();
            let sql = format!(
                "UPDATE operations SET has_kudgivt = TRUE WHERE tenant_id = $1 AND unko_no IN ({})",
                placeholders.join(", ")
            );
            let mut query = sqlx::query(&sql).bind(tenant_id);
            for unko_no in chunk {
                query = query.bind(unko_no);
            }
            let _ = query.execute(&state.pool).await;
        }
        tracing::info!("has_kudgivt updated: {} operations", kudgivt_unko_nos.len());
    }

    tracing::info!(
        "CSV split done: {} files uploaded (upload_id={}, tenant={})",
        csv_count, upload_id, tenant_id
    );
    Ok(())
}

/// Cloud Tasks から呼ばれる CSV 分割エンドポイント
async fn internal_split_csv(
    State(state): State<AppState>,
    Path(upload_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    tracing::info!("split-csv called: upload_id={}", upload_id);

    split_csv_from_r2(&state, upload_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({ "status": "ok", "upload_id": upload_id })))
}

/// R2 に保存済みの ZIP をダウンロード
async fn internal_download(
    State(state): State<AppState>,
    Path(upload_id): Path<Uuid>,
) -> Result<Response, (StatusCode, String)> {
    let record = sqlx::query_as::<_, (String, String)>(
        "SELECT r2_zip_key, filename FROM upload_history WHERE id = $1",
    )
    .bind(upload_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(internal_err)?
    .ok_or_else(|| (StatusCode::NOT_FOUND, format!("upload {} not found", upload_id)))?;

    let (r2_zip_key, filename) = record;

    let zip_bytes = state
        .storage
        .download(&r2_zip_key)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("R2 download failed: {e}")))?;

    // ASCII-safe filename fallback
    let safe_name = filename
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '.' || *c == '-' || *c == '_')
        .collect::<String>();
    let safe_name = if safe_name.is_empty() { "download.zip".to_string() } else { safe_name };

    Ok(Response::builder()
        .header("Content-Type", "application/zip")
        .header(
            "Content-Disposition",
            format!("attachment; filename=\"{}\"", safe_name),
        )
        .body(Body::from(zip_bytes))
        .unwrap())
}

/// R2 に保存済みの ZIP を再処理
async fn internal_rerun(
    State(state): State<AppState>,
    Path(upload_id): Path<Uuid>,
) -> Result<Json<UploadResponse>, (StatusCode, String)> {
    // upload_history から r2_zip_key を取得
    let record = sqlx::query_as::<_, (Uuid, String, String)>(
        "SELECT tenant_id, r2_zip_key, filename FROM upload_history WHERE id = $1",
    )
    .bind(upload_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(internal_err)?
    .ok_or_else(|| (StatusCode::NOT_FOUND, format!("upload {} not found", upload_id)))?;

    let (tenant_id, r2_zip_key, filename) = record;

    // R2 から ZIP をダウンロード
    let zip_bytes = state
        .storage
        .download(&r2_zip_key)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("R2 download failed: {e}")))?;

    tracing::info!("Rerun: upload_id={}, tenant={}, file={}", upload_id, tenant_id, filename);

    match process_zip(&state, tenant_id, upload_id, &filename, &zip_bytes).await {
        Ok(count) => {
            sqlx::query(
                "UPDATE upload_history SET status = 'completed', operations_count = $1 WHERE id = $2",
            )
            .bind(count)
            .bind(upload_id)
            .execute(&state.pool)
            .await
            .map_err(internal_err)?;

            Ok(Json(UploadResponse {
                upload_id,
                operations_count: count,
                status: "completed".to_string(),
            }))
        }
        Err(e) => {
            let _ = sqlx::query(
                "UPDATE upload_history SET status = 'failed', error_message = $1 WHERE id = $2",
            )
            .bind(e.to_string())
            .bind(upload_id)
            .execute(&state.pool)
            .await;

            Err((StatusCode::BAD_REQUEST, e.to_string()))
        }
    }
}

#[derive(Debug, Deserialize)]
struct RecalcFilter {
    year: i32,
    month: u32,
}

/// 月指定で再計算（R2の個別CSVから。SSEで進捗通知）
async fn internal_recalculate_all(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Query(params): Query<RecalcFilter>,
) -> Response<Body> {
    let tenant_id = auth_user.tenant_id;
    let year = params.year;
    let month = params.month;

    let (tx, rx) = tokio::sync::mpsc::channel::<String>(32);

    tokio::spawn(async move {
        let send = |json: serde_json::Value| {
            let tx = tx.clone();
            async move {
                let s = serde_json::to_string(&json).unwrap_or_default();
                let _ = tx.send(format!("data: {s}\n\n")).await;
            }
        };

        let month_start = match chrono::NaiveDate::from_ymd_opt(year, month, 1) {
            Some(d) => d,
            None => {
                send(serde_json::json!({"event":"error","message":"invalid year/month"})).await;
                return;
            }
        };
        let month_end = if month == 12 {
            chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1)
        } else {
            chrono::NaiveDate::from_ymd_opt(year, month + 1, 1)
        }
        .unwrap()
            - chrono::Duration::days(1);

        // 1. 指定月のoperationsを取得（KUDGURI情報）
        #[derive(sqlx::FromRow)]
        struct OpRow {
            unko_no: String,
            reading_date: chrono::NaiveDate,
            operation_date: Option<chrono::NaiveDate>,
            departure_at: Option<chrono::DateTime<chrono::Utc>>,
            return_at: Option<chrono::DateTime<chrono::Utc>>,
            driver_cd: Option<String>,
            total_distance: Option<f64>,
            drive_time_general: Option<i32>,
            drive_time_highway: Option<i32>,
            drive_time_bypass: Option<i32>,
        }

        let op_rows = match sqlx::query_as::<_, OpRow>(
            r#"SELECT DISTINCT o.unko_no, o.reading_date, o.operation_date,
                      o.departure_at, o.return_at,
                      d.driver_cd,
                      o.total_distance,
                      o.drive_time_general, o.drive_time_highway, o.drive_time_bypass
               FROM operations o
               LEFT JOIN drivers d ON d.id = o.driver_id AND d.tenant_id = o.tenant_id
               WHERE o.tenant_id = $1
                 AND (o.operation_date >= $2 AND o.operation_date <= $3
                      OR o.reading_date >= $2 AND o.reading_date <= $3)
               ORDER BY o.reading_date, o.unko_no"#,
        )
        .bind(tenant_id)
        .bind(month_start)
        .bind(month_end)
        .fetch_all(&state.pool)
        .await
        {
            Ok(o) => o,
            Err(e) => {
                send(serde_json::json!({"event":"error","message":format!("DB error: {e}")})).await;
                return;
            }
        };

        // OpRow → KudguriRow に変換
        let ops: Vec<KudguriRow> = op_rows.iter().map(|r| KudguriRow {
            unko_no: r.unko_no.clone(),
            reading_date: r.reading_date,
            operation_date: r.operation_date,
            office_cd: String::new(),
            office_name: String::new(),
            vehicle_cd: String::new(),
            vehicle_name: String::new(),
            driver_cd: r.driver_cd.clone().unwrap_or_default(),
            driver_name: String::new(),
            crew_role: 0,
            departure_at: r.departure_at.map(|dt| dt.naive_utc()),
            return_at: r.return_at.map(|dt| dt.naive_utc()),
            garage_out_at: None,
            garage_in_at: None,
            meter_start: None,
            meter_end: None,
            total_distance: r.total_distance,
            drive_time_general: r.drive_time_general,
            drive_time_highway: r.drive_time_highway,
            drive_time_bypass: r.drive_time_bypass,
            safety_score: None,
            economy_score: None,
            total_score: None,
            raw_data: serde_json::Value::Null,
        }).collect();

        let total = ops.len();
        send(serde_json::json!({"event":"progress","current":0,"total":total,"step":"start"})).await;

        // 2. R2から各運行のKUDGIVT.csvを取得
        let mut all_kudgivt: Vec<KudgivtRow> = Vec::new();
        let batch_size = 20;
        for batch_start in (0..total).step_by(batch_size) {
            let batch_end = (batch_start + batch_size).min(total);
            send(serde_json::json!({
                "event": "progress",
                "current": batch_end,
                "total": total,
                "step": "download"
            })).await;

            let futures: Vec<_> = ops[batch_start..batch_end]
                .iter()
                .map(|op| {
                    let r2_key = format!("{}/unko/{}/KUDGIVT.csv", tenant_id, op.unko_no);
                    let storage = state.storage.clone();
                    async move { (op.unko_no.clone(), storage.download(&r2_key).await) }
                })
                .collect();

            let results = futures::future::join_all(futures).await;
            for (unko_no, result) in results {
                match result {
                    Ok(bytes) => {
                        let csv_text = String::from_utf8_lossy(&bytes);
                        match parse_kudgivt(&csv_text) {
                            Ok(rows) => all_kudgivt.extend(rows),
                            Err(e) => tracing::warn!("KUDGIVT parse error {}: {e}", unko_no),
                        }
                    }
                    Err(e) => {
                        tracing::warn!("KUDGIVT not found for {}: {e}", unko_no);
                    }
                }
            }
        }

        if all_kudgivt.is_empty() {
            send(serde_json::json!({"event":"error","message":"KUDGIVTが見つかりません。先にCSV分割を実行してください。"})).await;
            return;
        }

        send(serde_json::json!({
            "event": "progress",
            "current": total,
            "total": total,
            "step": "calculate"
        })).await;

        // 2.5. KUDGFRYからフェリー時間を取得
        let ferry_minutes = load_ferry_minutes(&state, tenant_id, &ops).await;

        // 3. 再計算
        match calculate_daily_hours(&state, tenant_id, &ops, &all_kudgivt, &ferry_minutes, Some(tx.clone())).await {
            Ok(()) => {
                send(serde_json::json!({
                    "event": "done",
                    "total": total,
                    "success": total,
                    "failed": 0
                })).await;
            }
            Err(e) => {
                send(serde_json::json!({"event":"error","message":format!("計算エラー: {e}")})).await;
            }
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx)
        .map(|msg| Ok::<_, std::convert::Infallible>(msg));

    Response::builder()
        .status(200)
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("X-Accel-Buffering", "no")
        .body(Body::from_stream(stream))
        .unwrap()
}

/// pending_retry / failed のアップロード一覧
async fn list_pending_uploads(
    State(state): State<AppState>,
) -> Result<Json<Vec<serde_json::Value>>, (StatusCode, String)> {
    let rows = sqlx::query_as::<_, (Uuid, Uuid, String, String, Option<String>, chrono::DateTime<chrono::Utc>)>(
        r#"SELECT id, tenant_id, filename, status, error_message, created_at
           FROM upload_history
           WHERE status IN ('pending_retry', 'failed')
           ORDER BY created_at DESC
           LIMIT 50"#,
    )
    .fetch_all(&state.pool)
    .await
    .map_err(internal_err)?;

    let items: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(id, tenant_id, filename, status, error, created_at)| {
            serde_json::json!({
                "id": id,
                "tenant_id": tenant_id,
                "filename": filename,
                "status": status,
                "error_message": error,
                "created_at": created_at.to_rfc3339(),
            })
        })
        .collect();

    Ok(Json(items))
}

#[derive(Debug, Deserialize)]
struct RecalcDriverFilter {
    year: i32,
    month: u32,
    driver_id: Uuid,
}

/// 1ドライバー分の月次再計算（R2からKUDGIVT取得→再計算）
async fn recalculate_driver(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
    Query(params): Query<RecalcDriverFilter>,
) -> Response<Body> {
    let tenant_id = auth_user.tenant_id;
    let year = params.year;
    let month = params.month;
    let driver_id = params.driver_id;

    let (tx, rx) = tokio::sync::mpsc::channel::<String>(32);

    tokio::spawn(async move {
        let send = |json: serde_json::Value| {
            let tx = tx.clone();
            async move {
                let s = serde_json::to_string(&json).unwrap_or_default();
                let _ = tx.send(format!("data: {s}\n\n")).await;
            }
        };

        let month_start = match chrono::NaiveDate::from_ymd_opt(year, month, 1) {
            Some(d) => d,
            None => {
                send(serde_json::json!({"event":"error","message":"invalid year/month"})).await;
                return;
            }
        };
        let month_end = if month == 12 {
            chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1)
        } else {
            chrono::NaiveDate::from_ymd_opt(year, month + 1, 1)
        }
        .unwrap()
            - chrono::Duration::days(1);

        // driver_cd を取得
        let driver_cd: Option<String> = match sqlx::query_scalar(
            "SELECT driver_cd FROM drivers WHERE id = $1 AND tenant_id = $2",
        )
        .bind(driver_id)
        .bind(tenant_id)
        .fetch_optional(&state.pool)
        .await
        {
            Ok(d) => d,
            Err(e) => {
                send(serde_json::json!({"event":"error","message":format!("driver error: {e}")})).await;
                return;
            }
        };

        let Some(driver_cd) = driver_cd else {
            send(serde_json::json!({"event":"error","message":"ドライバーが見つかりません"})).await;
            return;
        };

        // 1. 該当ドライバーの operations を取得
        #[derive(sqlx::FromRow)]
        struct OpRow {
            unko_no: String,
            reading_date: chrono::NaiveDate,
            operation_date: Option<chrono::NaiveDate>,
            departure_at: Option<chrono::DateTime<chrono::Utc>>,
            return_at: Option<chrono::DateTime<chrono::Utc>>,
            total_distance: Option<f64>,
            drive_time_general: Option<i32>,
            drive_time_highway: Option<i32>,
            drive_time_bypass: Option<i32>,
        }

        let op_rows = match sqlx::query_as::<_, OpRow>(
            r#"SELECT DISTINCT o.unko_no, o.reading_date, o.operation_date,
                      o.departure_at, o.return_at,
                      o.total_distance,
                      o.drive_time_general, o.drive_time_highway, o.drive_time_bypass
               FROM operations o
               WHERE o.tenant_id = $1 AND o.driver_id = $2
                 AND (o.operation_date >= $3 AND o.operation_date <= $4
                      OR o.reading_date >= $3 AND o.reading_date <= $4)
               ORDER BY o.reading_date, o.unko_no"#,
        )
        .bind(tenant_id)
        .bind(driver_id)
        .bind(month_start)
        .bind(month_end)
        .fetch_all(&state.pool)
        .await
        {
            Ok(o) => o,
            Err(e) => {
                send(serde_json::json!({"event":"error","message":format!("DB error: {e}")})).await;
                return;
            }
        };

        let ops: Vec<KudguriRow> = op_rows.iter().map(|r| KudguriRow {
            unko_no: r.unko_no.clone(),
            reading_date: r.reading_date,
            operation_date: r.operation_date,
            office_cd: String::new(),
            office_name: String::new(),
            vehicle_cd: String::new(),
            vehicle_name: String::new(),
            driver_cd: driver_cd.clone(),
            driver_name: String::new(),
            crew_role: 0,
            departure_at: r.departure_at.map(|dt| dt.naive_utc()),
            return_at: r.return_at.map(|dt| dt.naive_utc()),
            garage_out_at: None,
            garage_in_at: None,
            meter_start: None,
            meter_end: None,
            total_distance: r.total_distance,
            drive_time_general: r.drive_time_general,
            drive_time_highway: r.drive_time_highway,
            drive_time_bypass: r.drive_time_bypass,
            safety_score: None,
            economy_score: None,
            total_score: None,
            raw_data: serde_json::Value::Null,
        }).collect();

        let total = ops.len();
        send(serde_json::json!({"event":"progress","current":0,"total":total,"step":"start","driver_cd":&driver_cd})).await;

        // 2. R2のZIPからKUDGIVT取得
        send(serde_json::json!({
            "event": "progress",
            "current": 0,
            "total": total,
            "step": "download"
        })).await;

        let all_kudgivt = match load_kudgivt_from_zips(&state, tenant_id, month_start, month_end).await {
            Ok(rows) => rows,
            Err(e) => {
                send(serde_json::json!({"event":"error","message":format!("KUDGIVT取得エラー: {e}")})).await;
                return;
            }
        };

        send(serde_json::json!({
            "event": "progress",
            "current": total,
            "total": total,
            "step": "calculate"
        })).await;

        // 2.5. KUDGFRYからフェリー時間を取得
        let ferry_minutes = load_ferry_minutes(&state, tenant_id, &ops).await;

        // 3. 再計算
        match calculate_daily_hours(&state, tenant_id, &ops, &all_kudgivt, &ferry_minutes, Some(tx.clone())).await {
            Ok(()) => {
                send(serde_json::json!({
                    "event": "done",
                    "total": total,
                    "driver_cd": &driver_cd
                })).await;
            }
            Err(e) => {
                send(serde_json::json!({"event":"error","message":format!("計算エラー: {e}")})).await;
            }
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx)
        .map(|msg| Ok::<_, std::convert::Infallible>(msg));

    Response::builder()
        .status(200)
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("X-Accel-Buffering", "no")
        .body(Body::from_stream(stream))
        .unwrap()
}

/// アップロード一覧
async fn list_uploads(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<Vec<serde_json::Value>>, (StatusCode, String)> {
    let rows = sqlx::query_as::<_, (Uuid, String, String, Option<String>, chrono::DateTime<chrono::Utc>, String)>(
        r#"SELECT id, filename, status, error_message, created_at, r2_zip_key
           FROM upload_history
           WHERE tenant_id = $1
           ORDER BY created_at DESC
           LIMIT 50"#,
    )
    .bind(auth_user.tenant_id)
    .fetch_all(&state.pool)
    .await
    .map_err(internal_err)?;

    let items: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(id, filename, status, error, created_at, r2_zip_key)| {
            serde_json::json!({
                "id": id,
                "filename": filename,
                "status": status,
                "error": error,
                "created_at": created_at,
                "r2_zip_key": r2_zip_key,
            })
        })
        .collect();

    Ok(Json(items))
}

/// 認証付きCSV分割エンドポイント
async fn split_csv_handler(
    State(state): State<AppState>,
    Extension(_auth_user): Extension<AuthUser>,
    Path(upload_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    tracing::info!("split-csv (auth) called: upload_id={}", upload_id);

    split_csv_from_r2(&state, upload_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({ "status": "ok", "upload_id": upload_id })))
}

/// 全completedアップロードのCSV分割（SSE進捗）
async fn split_csv_all_handler(
    State(state): State<AppState>,
    Extension(auth_user): Extension<AuthUser>,
) -> Response<Body> {
    let tenant_id = auth_user.tenant_id;

    let (tx, rx) = tokio::sync::mpsc::channel::<String>(32);

    tokio::spawn(async move {
        let send = |json: serde_json::Value| {
            let tx = tx.clone();
            async move {
                let s = serde_json::to_string(&json).unwrap_or_default();
                let _ = tx.send(format!("data: {s}\n\n")).await;
            }
        };

        // 未分割の運行のupload_idを特定
        let uploads: Vec<(Uuid, String)> = match sqlx::query_as(
            r#"SELECT DISTINCT uh.id, uh.filename
               FROM operations o
               JOIN upload_history uh ON uh.tenant_id = o.tenant_id
               WHERE o.tenant_id = $1 AND o.has_kudgivt = FALSE
                 AND uh.status = 'completed'
                 AND uh.r2_zip_key IS NOT NULL
               ORDER BY uh.filename"#,
        )
        .bind(tenant_id)
        .fetch_all(&state.pool)
        .await
        {
            Ok(u) => u,
            Err(e) => {
                send(serde_json::json!({"event":"error","message":format!("DB error: {e}")})).await;
                return;
            }
        };

        // ファイル名で重複排除
        let mut seen_filenames = std::collections::HashSet::new();
        let uploads: Vec<_> = uploads.into_iter().filter(|(_, f)| seen_filenames.insert(f.clone())).collect();

        let total = uploads.len();
        if total == 0 {
            send(serde_json::json!({"event":"done","total":0,"success":0,"failed":0})).await;
            return;
        }
        send(serde_json::json!({"event":"progress","current":0,"total":total,"step":"start"})).await;

        // 5並列でZIPを処理
        let success = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let failed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let done_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let zip_batch = 5;
        for chunk in uploads.chunks(zip_batch) {
            let futures: Vec<_> = chunk.iter().map(|(upload_id, _filename)| {
                let st = state.clone();
                let uid = *upload_id;
                let s = success.clone();
                let f = failed.clone();
                async move {
                    match split_csv_from_r2(&st, uid).await {
                        Ok(()) => { s.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
                        Err(e) => {
                            tracing::warn!("split failed for {}: {e}", uid);
                            f.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }).collect();

            futures::future::join_all(futures).await;

            let current = done_count.fetch_add(chunk.len(), std::sync::atomic::Ordering::Relaxed) + chunk.len();
            send(serde_json::json!({
                "event":"progress",
                "current": current,
                "total": total,
                "step": "split",
            })).await;
        }

        let success = success.load(std::sync::atomic::Ordering::Relaxed);
        let failed = failed.load(std::sync::atomic::Ordering::Relaxed);

        send(serde_json::json!({
            "event":"done",
            "total": total,
            "success": success,
            "failed": failed,
        })).await;
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx)
        .map(|msg| Ok::<_, std::convert::Infallible>(msg));

    Response::builder()
        .status(200)
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("X-Accel-Buffering", "no")
        .body(Body::from_stream(stream))
        .unwrap()
}
