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

## 差分修正の要領

### 修正の基本原則

1. **他ドライバーを壊さない**: 修正後は `bash test_and_deploy.sh` で全ドライバーの既存テストが通ることを確認
2. **1つずつ修正**: 複数の差分原因を同時に修正しない。1原因→テスト→コミットのサイクル
3. **根本原因の特定**: 差分の数値だけでなく、なぜその差が出るかを特定してから修正

### よくある差分パターンと修正箇所

| パターン | 根本原因 | 修正箇所 |
|---------|---------|---------|
| 始業/終業ズレ | workday境界の計算 | `determine_workdays()` / `split_segments_at_24h()` |
| 運転/小計 1分差 | イベント秒→分の丸め | event-level処理（`day_drive_secs`等の集計） |
| 小計にフェリー未控除 | KUDGFRY→301マッピング | フェリー控除セクション（`ferry_deduction`） |
| 重複小計ズレ | overlap方向・フェリー | overlap section（`post_process_day_map`） |
| 累計ズレ | 上流の日別差分の蓄積 | 累計の最初のズレ日を修正すれば下流も直る |
| 実働/時間外ズレ | 運転+荷役の差 or 8h控除 | event-level drive/cargo集計 |

### overlap（重複小計）の修正要領

- **定義**: 前workdayの始業+24h > 次workdayの始業 → その時間帯の拘束が重複
  - 例: 始業5:00→翌始業3:00 → 3:00〜5:00がoverlap window
- **chain vs 非chain**:
  - chain（`next_resets=false`）: 合算。フェリーは控除しない
  - 非chain（`next_resets=true`）: overlap windowにフェリーがあれば301 duration分を控除
- **フェリー控除**: KUDGFRY期間の秒数ではなく、対応する301イベントのduration_minutesで控除（web地球号互換）

### フェリー控除の修正要領

- フェリー控除 = KUDGFRY.csv存在時にその期間の301イベントdurationを差し引く
- 控除対象: `total_work_minutes`（小計）と `drive_minutes`（301と重なるdrive/cargo）
- 重複小計への控除: 非chain時のみ（overlap window内のフェリー301を控除）
- 丸め: KUDGFRY期間の秒計算ではなく301イベントのduration（整数分）を使う

### workday境界（始業/24h分割）の修正要領

- 休息基準: 540分(原則)、480分(長距離450km+の最後の休息)
- 24h上限: 始業から24h経過で強制日締め（`split_segments_at_24h`）
- 日帰属: 休息で分割 → 次の拘束開始が新始業。分割なし → 前の始業に帰属し続ける
- `multi_op_boundaries`: 複数workday時のevent分割境界。event-level処理で使用
- 注意: `split_segments_at_24h`はsegment開始基準。始業基準の24hとはズレる場合がある（既知制限）
