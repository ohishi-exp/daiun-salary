# Plan: 一瀬道広(1026)のCSV回帰テスト追加 & 差分特定

## Context
鈴木昭(1021)の回帰テストは0件差分で成功済み。一瀬道広(1026)でもCSVとシステム値のズレを特定したい。
一瀬のCSVデータは `拘束時間管理表 (1).csv` の77行目〜に存在（コード: 1026）。

## 課題
一瀬のCSVには**同一日に2行ある日**がある（日跨ぎ運行）：
- 2月2日: 2行（1:17出発 → 帰着、23:17出発 → 翌日帰着）
- 2月5日: 2行（同様パターン）
- 2月14日: 1行 + 休

CSVパーサー `parse_restraint_csv` は各行をそのまま `days` に push するため、一瀬は**28日以上のrow**になる。
一方 `build_sys_days_from_mock` は1日1行（28行）を生成。`detect_diffs` は `zip` で比較するため**位置がズレる**。

## 実装ステップ

- [ ] **Step 1**: `CSV_1026` 定数を追加（一瀬のCSVデータ、UTF-8変換済み）
  - ファイル: `src/routes/restraint_report.rs` のテストモジュール内

- [ ] **Step 2**: `test_parse_csv_1026` テスト追加
  - パース結果の行数・ドライバー名確認
  - 同一日2行のケースでdays数がいくつになるか確認

- [ ] **Step 3**: DBから一瀬(1026)の2026年2月データ取得
  - `SELECT day, drive_minutes, ...` from daily_work_hours
  - MockDwh配列として定数化

- [ ] **Step 4**: `test_compare_1026_with_db_mock` テスト追加
  - まず実行して差分を確認（最初は失敗するはず）
  - 差分出力から何がズレているか特定

- [ ] **Step 5**: 日跨ぎ（同一日2行）対応
  - `detect_diffs` または `build_sys_days_from_mock` を修正して、同一日複数行のCSVとマッチできるようにする
  - 方針: CSV側の同一日行を合算するか、システム側も複数行に分割するか → 差分を見てから決定

## 修正対象ファイル
- `src/routes/restraint_report.rs` — テスト追加 & 比較ロジック修正

## 検証方法
```bash
cargo test restraint_report -- --nocapture
```
差分が0件になれば完了。差分が出る場合はその内容から計算ロジックの問題を特定。
