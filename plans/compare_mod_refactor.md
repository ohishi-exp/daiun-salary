# compare/mod.rs リファクタリング計画

## 現状
- 3620行（うちテスト1830行、本体1790行）
- テスト74件、coverage 92%
- 抽出済み関数: ferry_break_overlap, ferry_drive_cargo_overlap, split_event_at_boundaries, find_event_workday, accumulate_daily_segment, parse_ferry_periods_from_text, process_parsed_data

## 目標
- 本体1790行 → 1200行以下（-33%）
- 巨大関数(build_day_map 500行, post_process_day_map 400行)の分割
- フェリーCSVパース重複の統合
- テストは壊さない（92% coverage維持）

## Step 1: フェリーCSVパース統合 (-50行)

`parse_ferry_minutes`(L695-730)と`FerryInfo::from_zip_files`(L916-975)が同じCSVを2回パース。
`parse_ferry_periods_from_text`（既に抽出済み）を使って統合。

```
before: parse_ferry_minutes + from_zip_files → 2回パース
after:  from_zip_files のみ。parse_ferry_periods_from_text を内部使用。
        parse_ferry_minutes は from_zip_files.ferry_minutes で代替。
```

- [ ] `FerryInfo::from_zip_files`で`parse_ferry_periods_from_text`を使う
- [ ] `parse_ferry_minutes`を削除、呼び出し元を`ferry_info.ferry_minutes`に変更
- [ ] テスト追加: FerryInfo構築のユニットテスト

## Step 2: post_process_day_map 分割 (-100行)

現在400行の1関数。3つのフェーズに分割:

```
post_process_day_map()
  ├── merge_same_day_entries()    ← 構内結合 (L1030-1095)
  ├── process_overlap_chain()     ← overlap計算 (L1096-1370)
  └── apply_ferry_deductions()    ← フェリー控除 (L1375-1415)
```

- [ ] `merge_same_day_entries(day_map, workday_boundaries)` 抽出
- [ ] `process_overlap_chain(day_map, ..., long_distance_unkos)` 抽出
- [ ] `apply_ferry_deductions(day_map, ferry_info, kudgivt_by_unko, classifications)` 抽出
- [ ] post_process_day_mapは3関数を順に呼ぶだけに

## Step 3: build_day_map 分割 (-100行)

現在500行の1関数。単一運行パスとmulti-opパスを分離:

```
build_day_map()
  ├── group_and_classify_ops()     ← workday_groups構築 (L1447-1530)
  ├── process_single_op_group()    ← 単一運行パス (L1533-1660)
  ├── process_multi_op_group()     ← multi-opパス (L1660-1780)
  └── aggregate_events_by_day()   ← event-level集計 (L1780-1940)
```

- [ ] `process_single_op_group(ops, ...)` 抽出
- [ ] `process_multi_op_group(ops, ...)` 抽出
- [ ] `aggregate_events_by_day(driver_cd, unko_nos, ...)` 抽出
- [ ] build_day_mapはオーケストレーションのみに

## Step 4: DayKey型エイリアス導入 (-30行)

`(String, NaiveDate, NaiveTime)` が30箇所以上で使用。

```rust
pub type DayKey = (String, NaiveDate, NaiveTime);
```

- [ ] 型エイリアス定義
- [ ] day_map, workday_boundaries, multi_wd_boundaries等を置換

## Step 5: process_parsed_data のCsvDayRow生成分離 (-50行)

L2040-2155のCsvDayRow生成ロジックを関数に:

```rust
fn build_csv_driver_data(
    day_map: &HashMap<DayKey, DayAgg>,
    workday_boundaries: &HashMap<DayKey, (NaiveDateTime, NaiveDateTime)>,
    driver_cd: &str, driver_name: &str,
    target_year: i32, target_month: u32,
) -> CsvDriverData
```

- [ ] 関数抽出
- [ ] テスト追加

## 検証（各ステップ後）

```bash
cargo test --lib -- compare::tests  # 74件全pass
cargo llvm-cov --lib -- compare::tests  # 92%維持

# リグレッション
cargo run --bin compare -- "test_data/拘束時間管理表_202602-1018-1021-1026.csv" "test_data/csvdata-202602-1018-1021-1026.zip"
cargo run --bin compare -- "test_data/拘束時間管理表_202602-all.csv" "test_data/csvdata-202602-1051.zip" -d 1051
cargo run --bin compare -- "test_data/拘束時間管理表_202602-all.csv" "test_data/csvdata-202602-1049.zip" -d 1049
```

## リスク
- Step 2-3は大きな関数移動。ライフタイム問題が出る可能性あり
- Step 3のmulti-opパスは複雑なクロージャを含む。抽出時に引数が増える
- 各ステップ後に必ず全テスト実行。1ステップ=1コミット
