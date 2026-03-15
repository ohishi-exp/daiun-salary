use axum::{
    body::Body,
    extract::{Multipart, Path, State},
    http::StatusCode,
    response::Response,
    routing::{post, get},
    Extension, Json, Router,
};
use serde::Serialize;
use uuid::Uuid;

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
    calculate_daily_hours(state, tenant_id, &rows, &kudgivt_rows).await?;
    tracing::info!("calculate_daily_hours done (tenant={})", tenant_id);

    // 6. Enqueue CSV split task (async, non-blocking)
    if let Err(e) = enqueue_csv_split(state, upload_id).await {
        tracing::warn!("Cloud Tasks enqueue failed (will not block): {e}");
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

async fn calculate_daily_hours(
    state: &AppState,
    tenant_id: Uuid,
    rows: &[KudguriRow],
    kudgivt_rows: &[KudgivtRow],
) -> Result<(), anyhow::Error> {
    use std::collections::HashMap;

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
    }

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

                    let entry = day_map
                        .entry((row.driver_cd.clone(), ds.date))
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

    // 4. Persist to DB
    for ((_driver_cd, work_date), agg) in &day_map {
        let Some(driver_id) = agg.driver_id else {
            continue;
        };

        let rest_minutes = (agg.total_work_minutes - agg.total_labor_minutes).max(0);

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
                total_distance, operation_count, unko_nos
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"#,
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
    }

    Ok(())
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

    let mut csv_count = 0usize;
    for (name, bytes) in &files {
        if name.to_lowercase().ends_with(".csv") {
            let utf8_text = csv_parser::decode_shift_jis(bytes);
            let header = csv_parser::csv_header(&utf8_text);
            let grouped = csv_parser::group_csv_by_unko_no(&utf8_text);

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
                let _ = state
                    .storage
                    .upload(&key, content.as_bytes(), "text/csv")
                    .await;
                csv_count += 1;
            }
        }
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
