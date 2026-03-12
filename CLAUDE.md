# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

北海大運（Hokkaido Daiun）の給与管理システム。Excel ベースの給与計算を Rust バックエンドに移行するプロジェクト。

## Tech Stack

- **Language:** Rust
- **Web Framework:** Axum
- **Database:** SQLite (dev) / PostgreSQL or Supabase (prod), via SQLx
- **Excel I/O:** calamine (read), rust_xlsxwriter (write)
- **Key crates:** chrono, rust_decimal, serde, tracing, thiserror, tokio

## Build & Test Commands

```bash
cargo build              # ビルド
cargo test               # 全テスト実行
cargo test <test_name>   # 単一テスト実行
cargo run                # サーバー起動
cargo clippy             # lint
cargo fmt                # フォーマット
```

## Architecture

```
src/
├── main.rs              # Axum サーバーエントリポイント
├── lib.rs
├── domain/              # ドメインモデル（Employee, Attendance, PayrollPeriod 等）
├── engine/              # 給与計算エンジン（コアロジック）
│   ├── time_calc.rs     # 拘束・労働・深夜時間計算
│   ├── overtime.rs      # 時間外・深夜手当
│   ├── holiday.rs       # 休日出勤手当（法定/法定外）
│   ├── allowance.rs     # 運行費・種別・地方手当・調整手当
│   ├── summary.rs       # 月次集計
│   └── payroll.rs       # 総支給額計算
├── repository/          # DB アクセス層（SQLx CRUD）
├── api/                 # Axum ハンドラ・ルーティング・DTO
└── import/              # Excel インポート（calamine）
```

## Domain-Specific Rules

### 時間計算
- 深夜時間帯は 22:00〜翌 05:00。日跨ぎは `NaiveDateTime` で管理
- 1日最大3シフト（`Vec<Shift>`）を合算
- 「明け」（`WorkType::Return`）の前日は時間外を計上しない
- 変形労働時間制：月間法定労働時間 173.8h を基準に時間外判定

### 金額計算
- Excel の ROUND 関数に合わせて `(x as f64).round() as i64` で丸める
- `rust_decimal` 使用時は `Decimal::round(0)`
- 時間外は 60h/80h 枠で割増率が変わる（25% → 50%）
- 法定休日手当は 135% 割増、法定最低額チェックあり

### Excel 互換
- インポート時に `#REF!` エラーはスキップしてログ出力
- エクスポートは現行 Excel フォーマットと互換にする

## Design Reference

詳細な設計書: [docs/給与管理_Rust実装計画.md](docs/給与管理_Rust実装計画.md)
- ドメインモデル定義（struct/enum の全フィールド）
- DB スキーマ（SQL DDL）
- API エンドポイント一覧
- 計算ロジックの擬似コード

## plans
plansフォルダに計画したplanはファイルとして保存
終了したplanはplans/completedに移動
plansはチェックリスト形式で記載
実行後チェックする

