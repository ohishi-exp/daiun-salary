# CLAUDE.md

## Project Overview
北海大運（Hokkaido Daiun）給与管理システム。Excel の給与計算を Rust/Axum へ移行。

## Tech Stack
Rust / Axum / SQLx（SQLite dev, PostgreSQL or Supabase prod）/ calamine（Excel読）/
rust_xlsxwriter（Excel書）/ chrono, rust_decimal, serde, tracing, thiserror, tokio

## Build & Test Commands
```bash
cargo build / cargo test / cargo test <name> / cargo run / cargo clippy / cargo fmt
```

## Architecture
```
src/
├── main.rs / lib.rs
├── domain/          # ドメインモデル
├── engine/          # 給与計算（time_calc/overtime/holiday/allowance/summary/payroll）
├── compare/mod.rs   # 拘束時間管理表CSV比較（コア計算、~3600行）
├── repository/      # DB アクセス層（SQLx CRUD）
├── api/             # Axum ハンドラ・ルーティング・DTO
└── import/          # Excel インポート（calamine）
```
詳細（compare/mod.rs 関数構造・ドメインルール・Known Bugs）は daiun-salary-map skill を参照。
設計書: `docs/給与管理_Rust実装計画.md`

## Related / Deploy
- フロントエンド: `/home/yhonda/js/nuxt-dtako-admin`（Nuxt 4。`npx nuxi build && npx wrangler deploy`）
- バックエンド: `./deploy.sh`（Docker build → GCP Artifact Registry push → Cloud Run deploy, asia-northeast1）

## Workflow
- 変更完了後は、AskUserQuestion ツールを使ってデプロイするか確認してから実行すること（勝手にデプロイしない）
- デプロイ時は既存のスクリプト（`./deploy.sh` 等）をそのまま使うこと。手動でコマンドを組み立てない

## plans
plansフォルダに計画したplanはファイルとして保存。終了したplanはplans/completedに移動。plansはチェックリスト形式で記載し、実行後チェックする。
</content>
