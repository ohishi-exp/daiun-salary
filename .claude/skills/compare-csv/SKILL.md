---
name: compare-csv
description: daiun-salary プロジェクトの拘束時間管理表CSV比較テスト。2つのCSVファイルの差分を検出し、ドライバーごとに始業・終業・運転・小計・合計・累計・実働・時間外・深夜の値を比較する。「CSV比較」「拘束時間比較」「compare csv」「差分確認」等で発動。
---

# 拘束時間管理表 CSV比較

## ワークフロー

### 1. 比較実行

```bash
# デフォルト: test_data内の参照CSVサマリー
bash .claude/skills/compare-csv/scripts/run_compare.sh

# 2ファイル比較
bash .claude/skills/compare-csv/scripts/run_compare.sh reference.csv system.csv

# ドライバー指定
bash .claude/skills/compare-csv/scripts/run_compare.sh reference.csv system.csv -d 1026
```

### 2. 差分分析

差分が出た場合、以下を確認:
- 始業/終業: 時刻フォーマット差（`1:17` vs `01:17`）→ 正規化済みなら実値差
- 運転/小計/合計: イベント集計ロジック（`src/routes/upload.rs` の `calculate_daily_hours`）
- 累計: 前日までの合計の蓄積ズレ → 最初の差分日を特定
- 深夜: 22:00-05:00 のセグメント計算（`calc_late_night_mins`）
- 時間外: 8h超過分の計算

### 3. ロジック修正→再比較

1. `src/bin/compare.rs` の比較ロジックまたは `src/routes/upload.rs` の集計ロジックを修正
2. `cargo run --bin compare -- reference.csv system.csv` で即座に再確認
3. exit code 0 = 差分なし、1 = 差分あり

## テストデータ

- 参照CSV: `test_data/拘束時間管理表_202602-1021-1026.csv`（web地球号出力）
- ドライバー: 鈴木昭(1021)、一瀬道広(1026)
- 期間: 2026年2月

## 比較対象フィールド

始業、終業、運転、重複運転、小計、重複小計、合計、累計、実働、時間外、深夜
