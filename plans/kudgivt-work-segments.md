# Plan: KUDGIVT活用による拘束時間・始業終業の正確な計算

## Context

現在 `calculate_daily_hours()` は KUDGURI の `departure_at→return_at` を1つの連続ブロックとして日跨ぎ分割している。複数日にまたがる運行では中間日の拘束が24時間になってしまう。

KUDGIVT.csv（ZIPに同梱）にはイベント単位の詳細ログがあり、302=休息イベントで勤務区間を分割できる。また、各イベントの区間時間から実際の労働時間を算出できる。

### 目的
1. 休息(302)で勤務区間を分割し、始業/終業を出す（1日複数回あり得る）
2. 拘束時間 = 勤務区間の合計（休息を除く）
3. 労働時間 = 勤務イベント（運転・積み・降し等）の合計
4. イベント分類を設定可能にする（将来のイベント追加に対応）

---

## チェックリスト

### 1. イベント分類テーブル
- [ ] `migrations/008_event_classifications.sql` 作成

```sql
CREATE TABLE IF NOT EXISTS event_classifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    event_cd TEXT NOT NULL,
    event_name TEXT NOT NULL,
    classification TEXT NOT NULL,  -- 'work', 'rest_split', 'break', 'ignore'
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(tenant_id, event_cd)
);
```

デフォルト分類:
| event_cd | event_name | classification |
|----------|-----------|---------------|
| 110 | IG-Moving(運転) | work |
| 202 | 積み | work |
| 203 | 降し | work |
| 302 | 休息 | rest_split |
| 301 | 休憩 | break |
| 101 | 実車 | ignore |
| 103 | 高速道 | ignore |
| 412 | アイドリング | ignore |
| 413 | 連続運転 | ignore |
| 402 | 急減速 | ignore |
| 405 | 一般道速度オーバー | ignore |
| 409 | 実車高速時エンジン回転オーバー | ignore |
| 410 | 空車低速時エンジン回転オーバー | ignore |

### 2. 勤務区間テーブル
- [ ] `migrations/009_daily_work_segments.sql` 作成

```sql
CREATE TABLE IF NOT EXISTS daily_work_segments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    driver_id UUID NOT NULL REFERENCES drivers(id),
    work_date DATE NOT NULL,
    unko_no TEXT NOT NULL,
    segment_index INTEGER NOT NULL DEFAULT 0,
    start_at TIMESTAMPTZ NOT NULL,    -- 始業
    end_at TIMESTAMPTZ NOT NULL,      -- 終業
    work_minutes INTEGER NOT NULL,    -- 拘束時間（区間）
    labor_minutes INTEGER NOT NULL DEFAULT 0,  -- 労働時間（勤務イベント合計）
    late_night_minutes INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dws_driver_date
    ON daily_work_segments(tenant_id, driver_id, work_date);
```

### 3. KUDGIVT パーサー
- [ ] `src/csv_parser/kudgivt.rs` 新規作成

```rust
pub struct KudgivtRow {
    pub unko_no: String,
    pub reading_date: NaiveDate,
    pub driver_cd: String,
    pub driver_name: String,
    pub crew_role: i32,
    pub start_at: NaiveDateTime,       // 開始日時
    pub event_cd: String,              // イベントCD
    pub event_name: String,            // イベント名
    pub duration_minutes: Option<i32>, // 区間時間
    pub section_distance: Option<f64>, // 区間距離
    pub raw_data: serde_json::Value,
}
```

- kudguri.rs のパターン（ColumnIndex, build_column_index, find_col/require_col）を踏襲
- 必須カラム: 運行NO, 読取日, 乗務員CD1, 乗務員名１, 対象乗務員区分, 開始日時, イベントCD, イベント名
- オプション: 区間時間, 区間距離, 事業所CD, 車輌CD 等
- [ ] `src/csv_parser/mod.rs` に `pub mod kudgivt;` 追加

### 4. 勤務区間分割ロジック
- [ ] `src/csv_parser/work_segments.rs` 新規作成

```rust
pub struct WorkSegment {
    pub start: NaiveDateTime,  // 始業
    pub end: NaiveDateTime,    // 終業
    pub labor_minutes: i32,    // 勤務イベントの合計分数
}

pub struct DailyWorkSegment {
    pub date: NaiveDate,
    pub start: NaiveDateTime,
    pub end: NaiveDateTime,
    pub work_minutes: i32,      // 拘束（区間の長さ）
    pub labor_minutes: i32,     // 労働（勤務イベント合計）
    pub late_night_minutes: i32,
}
```

**アルゴリズム:**

`split_by_rest(dep, ret, events, classifications) -> Vec<WorkSegment>`:
1. events を `start_at` でソート
2. 302(rest_split) イベントで区間を分割:
   - 休息開始 = 終業、休息終了(start_at + duration) = 次の始業
3. 各区間内の work 分類イベントの `duration_minutes` を合計 → `labor_minutes`
4. 302 イベントなしの場合 → departure→return の1区間（後方互換）

`split_segments_by_day(segments) -> Vec<DailyWorkSegment>`:
- 各区間を0:00境界で日分割
- `calc_late_night_mins()` を upload.rs から移動して再利用
- labor_minutes は日ごとに按分（work_minutes比）

- [ ] `src/csv_parser/mod.rs` に `pub mod work_segments;` 追加

### 5. upload.rs 改修
- [ ] `src/routes/upload.rs` の `process_zip()` 改修

変更点:
1. ZIPから KUDGIVT.csv も検索・パース（オプション、なければ従来動作）
2. KUDGIVT行を `unko_no` でグループ化
3. DB から `event_classifications` を取得
4. `calculate_daily_hours()` にKUDGIVT データと分類を渡す

- [ ] `calculate_daily_hours()` 改修

変更点:
1. 各KUDGURI行に対し、対応するKUDGIVT 302イベントで勤務区間を分割
2. `split_by_rest()` → `split_segments_by_day()` で日別区間を取得
3. DayAgg に区間情報を蓄積
4. `daily_work_segments` テーブルに詳細行をINSERT
5. `daily_work_hours` は集約値（total_work_minutes = 拘束合計, total_drive_minutes は labor_minutes の合計に変更検討）
6. `calc_late_night_mins()` を work_segments.rs に移動

### 6. モデル追加
- [ ] `src/db/models.rs` に `DailyWorkSegment` 構造体追加
- [ ] `EventClassification` 構造体追加

### 7. API 更新
- [ ] `src/routes/daily_hours.rs` にセグメント取得エンドポイント追加
  - `GET /daily-hours/:driver_id/:date/segments` → 始業/終業一覧
- [ ] `src/routes/csv_proxy.rs` に `"kudgivt"` マッピング追加（1行）

### 8. テスト
- [ ] kudgivt.rs: パーステスト、不足カラムエラーテスト
- [ ] work_segments.rs:
  - 302イベントなし → 1区間
  - 302イベント1件 → 2区間
  - 複数日運行の分割テスト（2/24 10:13 → 2/27 16:00 の例）
  - 日跨ぎ分割テスト
  - 深夜時間計算テスト
  - labor_minutes 計算テスト

---

## 対象ファイル

| ファイル | 操作 |
|---------|------|
| `migrations/008_event_classifications.sql` | 新規 |
| `migrations/009_daily_work_segments.sql` | 新規 |
| `src/csv_parser/kudgivt.rs` | 新規 |
| `src/csv_parser/work_segments.rs` | 新規 |
| `src/csv_parser/mod.rs` | 修正（mod追加） |
| `src/routes/upload.rs` | 修正（コア改修） |
| `src/db/models.rs` | 修正（モデル追加） |
| `src/routes/daily_hours.rs` | 修正（エンドポイント追加） |
| `src/routes/csv_proxy.rs` | 修正（1行追加） |

## 再利用する既存コード
- `src/csv_parser/kudguri.rs` のパーサーパターン（ColumnIndex, build_column_index等）
- `src/csv_parser/mod.rs` の `extract_zip()`, `decode_shift_jis()`, `group_csv_by_unko_no()`
- `src/routes/upload.rs` の `calc_late_night_mins()` → work_segments.rs に移動

## 検証方法
1. `cargo build` でコンパイル確認
2. `cargo test` で全テスト通過
3. `csvdata (19).zip` をアップロードし、梅津政弘の2/24-2/27運行データで:
   - 拘束時間が24時間ではなく実際の勤務時間になること
   - 各日に始業/終業が正しく記録されること
   - 深夜時間が勤務区間内のみで計算されること
4. KUDGIVT.csvなしのZIPでも従来通り動作すること（後方互換）
