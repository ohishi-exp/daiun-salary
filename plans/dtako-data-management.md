# デジタコデータ管理システム 実装計画

## Context

北海大運のデジタコ（矢崎製）CSV（ZIP/Shift-JIS）をアップロード・管理・閲覧するWebシステム。
rust-alc-api / alc-app のアーキテクチャパターンを踏襲。新規Supabaseプロジェクト。

## Phase 1: プロジェクト基盤 + 認証

- [x] Cargo.toml + src/main.rs (Axum server scaffolding)
- [x] auth/google.rs, auth/jwt.rs — rust-alc-api から移植
- [x] middleware/auth.rs — require_jwt
- [x] storage/ (StorageBackend trait + R2Backend) — rust-alc-api から移植
- [x] db/models.rs + db/tenant.rs
- [x] routes/auth.rs (Google OAuth code exchange + JWT + refresh)
- [x] migrations 001 (tenants, users)
- [x] migrations 002 (offices, vehicles, drivers マスタ)
- [x] Supabaseプロジェクト + マイグレーション適用 + RLS確認済み
- [x] cargo build 通す（残りの stub ファイル作成）

## Phase 2: CSV パース + アップロード

- [x] csv_parser/mod.rs (ZIP展開 + Shift-JIS→UTF-8)
- [x] csv_parser/kudguri.rs (KUDGURI.csv → operations行パース + テスト)
- [x] migrations 003 (operations)
- [x] migrations 004 (upload_history)
- [x] migrations 005 (daily_work_hours)
- [x] migrations 006 (RLS ポリシー)
- [x] routes/upload.rs: ZIP受信→R2保存→KUDGURI DB投入→マスタ自動upsert→daily_work_hours計算
- [x] 再アップロード対応（同一運行NO → DELETE + re-insert）

## Phase 3: 運行データAPI + フロントエンド

- [x] routes/operations.rs (一覧/詳細/削除)
- [x] routes/csv_proxy.rs (R2 CSV → JSON変換: events, tolls, ferries, speed)
- [x] routes/drivers.rs, vehicles.rs
- [x] routes/daily_hours.rs
- [x] Nuxt 4 フロントエンド scaffold (nuxt.config.ts, wrangler.toml) → /home/yhonda/js/nuxt-dtako-admin
- [x] login.vue + auth/callback.vue + useAuth.ts
- [x] utils/api.ts (centralized API client)
- [x] upload.vue (UploadZone.vue)
- [x] operations/index.vue (運行一覧 + OperationTable.vue)
- [x] operations/[unko_no].vue (運行詳細)
- [x] daily-hours/index.vue (日別労働時間)
- [x] nuxt build 成功確認

## Phase 4: 可視化 + 仕上げ

- [ ] SpeedChart.vue (速度分布グラフ)
- [ ] ScoreCard.vue (評価点表示)
- [ ] EventTimeline.vue (イベントタイムライン)
- [x] 労働時間集計ページ (daily-hours/index.vue)
- [ ] CSVダウンロード機能
- [ ] デプロイ設定 (Cloud Run + Cloudflare Workers)

## DB設計

**DBに格納**: operations (KUDGURI.csv) + upload_history + daily_work_hours + マスタ
**R2に保存**: 全5 CSV (UTF-8変換済み、運行NO毎) + 元ZIP

### 主要テーブル
- `operations`: 粒度 (unko_no, crew_role)、2マン時は2レコード、raw_data JSONB に107列全保持
- `daily_work_hours`: ドライバー×日毎の労働時間集計
- R2キー: `{tenant_id}/unko/{unko_no}/{filename}.csv`

## 参照ファイル

- `/home/yhonda/rust/rust-alc-api/src/` — auth, storage, middleware, routes パターン
- `/home/yhonda/js/alc-app/web/app/` — frontend auth, API client パターン
