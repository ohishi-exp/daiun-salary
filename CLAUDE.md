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
├── compare/             # 拘束時間管理表CSV比較（コア計算モジュール）
│   └── mod.rs           # ~3600行（テスト1830行含む）
├── repository/          # DB アクセス層（SQLx CRUD）
├── api/                 # Axum ハンドラ・ルーティング・DTO
└── import/              # Excel インポート（calamine）
```

### compare/mod.rs 関数構造

```
process_zip() / process_parsed_data()  ← エントリポイント
  ├── build_day_map()                  ← KUDGURI/KUDGIVTからday_map構築
  │     ├── single-op / multi-op パス  ← workday分割・セグメント生成
  │     └── aggregate_events_by_day()  ← イベント直接集計（秒→分変換）
  ├── post_process_day_map()           ← 後処理オーケストレーター
  │     ├── merge_same_day_entries()   ← 構内結合（異運行・gap<180分）
  │     ├── process_overlap_chain()    ← 24hチェーン・overlap計算
  │     └── apply_ferry_deductions()   ← フェリー控除
  └── build_csv_driver_data()          ← day_map→CsvDayRow変換
```

- **DayKey**: `type DayKey = (String, NaiveDate, NaiveTime)` — (driver_cd, work_date, start_time)
- **DayAgg**: 日別集計データ（運転/荷役/休憩/深夜/overlap等の分数）
- **FerryInfo**: フェリー控除用データ（`from_zip_files`で構築、`parse_ferry_periods_from_text`使用）

## Domain-Specific Rules

### 時間計算
- 深夜時間帯は 22:00〜翌 05:00。日跨ぎは `NaiveDateTime` で管理
- 1日最大3シフト（`Vec<Shift>`）を合算
- 「明け」（`WorkType::Return`）の前日は時間外を計上しない
- 変形労働時間制：月間法定労働時間 173.8h を基準に時間外判定

### web地球号（デジタコ管理システム）設定値
CSV出力元のweb地球号の就業時間管理マスター設定。拘束時間管理表の計算はこれに準拠する。

#### 就業時間常設定
- 所定労働時間: 08:00
- 深夜時間: 22:00〜05:00
- 深夜時間管理方法: 法定内・法定外時間に含めて管理する
- 控除時間: 01:00（就業時間管理マスターの設定値だが、**拘束時間管理表の拘束時間小計には適用されない**。拘束時間小計 = 運転+荷役+休憩（全イベント合計）- フェリー乗船時間 - 休息時間）
- フェリー控除: KUDGFRY.csvにフェリー乗船データがある場合、その時間を拘束時間から控除する。フェリー時間はKUDGIVTで301（休憩）として記録されるが、KUDGFRY存在時にその分を差し引く

#### 就業時間集計設定
- 集計開始判定: 最初の作業開始時刻
- 集計終了判定: 最後の作業終了時刻
- 就業日判定: 終業日を就業日とする
- 日締め判定: 休息の開始時刻（複数日運行のみ適用ではない）
- 日締め時刻: 00:00〜24:00
- 休息未取得時例外: 集計開始時刻の24時間後を日締め時刻とする

#### 締め日・時間丸め設定
- 月締め日: 31日
- 計上基準: **運行単位で、集計開始月に計上する**（日跨ぎ運行は出発日に帰属）
- 月締め時刻: 24:00
- 丸め単位時間: 1分

#### 集計基準設定（令和6年4月改正基準）
- カード未挿入時間: 180分以上で休息期間とする
- 休息期間定義: **休息基準を満たした場合、次の拘束開始日時を新規始業とする**
  - [原則] 連続540分以上
  - [例外] 連続480分以上（宿泊を伴う長距離貨物運送の例外基準。運行終了後、連続720分以上）
  - [特例] 1回あたり連続180分以上の休息期間の累計が 2分割=600分以上、3分割=720分以上
- 日帰属ルール（始業ベース）:
  - 休息基準を満たした場合 → 次の拘束開始 = **新規始業** → 新しい日の開始
  - 休息基準を満たさない場合 → 前の始業の日に帰属し続ける
  - **24時間が最大**: 始業から24h経過で強制日締め（休息未取得時例外）
- 時間外管理: 8時間00分までを所定労働時間とする
- 深夜管理: 22:00〜05:00
- 出社退社日時調整: 出社〜退社が重複する場合、自動調整（カード乗換対応）
- 構内作業時間管理: 出社〜退社間を拘束時間として管理する（休息は除く）→ **未チェック**
- 出社〜出庫間/帰庫〜退社間/複数運行間: 構内→時間として集計
- 休日行作成調整: 稼働日が連続する場合に原則1980分、休息期間が適応された場合1800分以上確保されていれば「休」行を作成する

### 金額計算
- Excel の ROUND 関数に合わせて `(x as f64).round() as i64` で丸める
- `rust_decimal` 使用時は `Decimal::round(0)`
- 時間外は 60h/80h 枠で割増率が変わる（25% → 50%）
- 法定休日手当は 135% 割増、法定最低額チェックあり

### Excel 互換
- インポート時に `#REF!` エラーはスキップしてログ出力
- エクスポートは現行 Excel フォーマットと互換にする

### 拘束時間項目設定（イベント→カテゴリマッピング）
- 運転(201=走行) → **運転**
- 積み(202) → **荷役**
- 降し(203) → **荷役**
- その他 → **荷役**
- 構内 → **荷役**
- 待機 → 別カラム（拘束時間に含むが運転・荷役・休憩には分類しない）
- 休憩(301) → **休憩**
- 休息(302) → **非拘束時間**（拘束時間から除外）

### 拘束時間小計の計算式
```
拘束時間小計 = Σ運転 + Σ荷役 + Σ休憩 - フェリー乗船時間
            = 全イベント合計(201+202+203+301) - KUDGFRY時間 - 休息(302)
実働時間 = Σ運転 + Σ荷役
時間外 = max(0, 実働時間 - 8:00)
深夜時間 = 22:00〜05:00の拘束時間（セグメントベース）
```

### R2ストレージ構造（Cloudflare R2）
- バケット: `ohishi-dtako`
- 運行単位のCSVデータ: `{tenant_id}/unko/{unko_no}/`
  - `KUDGIVT.csv` — イベントデータ（運転/荷役/休憩/休息の時系列）
  - `KUDGURI.csv` — 運行メタデータ（出社退社、走行距離等）
  - `KUDGFRY.csv`, `KUDGFUL.csv`, `SOKUDODATA.csv`
- アップロードZIP: `{tenant_id}/uploads/{upload_id}/{uuid}.zip`
- R2アクセス: `.env` の `R2_ACCESS_KEY`, `R2_SECRET_KEY`, `R2_ACCOUNT_ID`

## Design Reference

詳細な設計書: [docs/給与管理_Rust実装計画.md](docs/給与管理_Rust実装計画.md)
- ドメインモデル定義（struct/enum の全フィールド）
- DB スキーマ（SQL DDL）
- API エンドポイント一覧
- 計算ロジックの擬似コード

## Known Bugs（既知バグ）

拘束時間管理表の比較で検出された既知バグ。`src/compare/mod.rs` の `KNOWN_BUGS` に登録済み。

- **#1 休息基準未達で終業扱い** ([Issue #3](https://github.com/ohishi-exp/daiun-salary/issues/3))
  - 影響: 1039 (2/21-22, 15件)
  - 原因: determine_workdays で休息基準未達の休息が終業扱いになる
- **#2 24h分離バグ: 運行内休息でshigyo未リセット** ([Issue #2](https://github.com/ohishi-exp/daiun-salary/issues/2))
  - 影響: 1068 (2/2, 28件), 1041 (2/4, 96件)
  - 原因: group_operations_into_work_days の since_shigyo 24hチェックが、長距離運行内の休息を考慮せず誤発動
- **#3 長距離480例外: 24h境界手前の休息が分割されない**
  - 影響: 1069 (2/4, 6件)
  - 原因: 長距離運行で480≤休息<540の休息が24h境界の**手前に完全に収まる**場合、540閾値では分割されず24hルールでも拾えない。24h境界を**跨ぐ**場合は24hルールが正しく処理する
  - 条件: `determine_workdays` で `rest_end < max_end`（休息が24h内に収まる）かつ `rest_dur < 540`（原則未達）かつ `rest_dur >= 480`（長距離例外基準）
  - 注意: 480を全休息に適用すると1051の537分休息（24h境界跨ぎで正しく処理済み）が誤分割される。週2回制限との相互作用も要調査

## Related Projects

- **フロントエンド:** `/home/yhonda/js/nuxt-dtako-admin` — Nuxt 4 + Nuxt UI の管理画面。本バックエンドの API を呼び出す

## Deploy

- **バックエンド:** `./deploy.sh` — Docker build → GCP Artifact Registry push → Cloud Run deploy (asia-northeast1)
- **フロントエンド:** `cd /home/yhonda/js/nuxt-dtako-admin && npx nuxi build && npx wrangler deploy` — Cloudflare Workers

## Workflow

- 変更完了後は、AskUserQuestion ツールを使ってデプロイするか確認してから実行すること（勝手にデプロイしない）
- デプロイ時は既存のスクリプト（`./deploy.sh` 等）をそのまま使うこと。手動でコマンドを組み立てない

## plans
plansフォルダに計画したplanはファイルとして保存
終了したplanはplans/completedに移動
plansはチェックリスト形式で記載
実行後チェックする

