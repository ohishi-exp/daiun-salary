# Plan: CSV比較CLIツール

## Context

拘束時間管理表のCSV同士を比較する小さいCLIを作る。DB依存なし。
既存コードとの共有ライブラリ化は `AppState` 等の依存が深いため行わず、
純粋な比較ロジック（~175行）をCLI内にコピーして完全独立させる。

## 実装手順

### Step 1: `src/bin/compare.rs` 新規作成
- [ ] 以下の純粋関数・構造体を `restraint_report.rs` からコピー:
  - `CsvDayRow`, `CsvDriverData`, `DiffItem` 構造体
  - `parse_restraint_csv()` — CSV解析
  - `detect_diffs_csv()` — CsvDayRow同士の差分検出
  - `fmt_min()`, `parse_hhmm()` — 時間フォーマット
- [ ] CLI: 2ファイル比較 or 1ファイル内ドライバー指定

### Step 2: `Cargo.toml` に `[[bin]]` 追加
- [ ] `[[bin]] name = "compare" path = "src/bin/compare.rs"` 追加
- [ ] 追加依存: `encoding_rs` のみ（既存）

### Step 3: 検証
- [ ] `cargo build --bin compare`
- [ ] `cargo run --bin compare -- test_data/拘束時間管理表_202602-1021-1026.csv`
- [ ] `cargo test` — 既存テスト影響なし

## CLI仕様

```bash
# 1ファイル内の全ドライバー表示
cargo run --bin compare -- reference.csv system.csv

# 1ファイルに2ドライバーある場合（ドライバー間比較ではなくsummary表示）
cargo run --bin compare -- reference.csv

# ドライバー指定
cargo run --bin compare -- reference.csv system.csv --driver 1026
```

**出力例:**
```
=== 一瀬　道広 (1026) ===
  2月2日  運転:    csv=7:05  sys=7:10  ← 差分
  2月2日  小計:    csv=12:20 sys=12:25 ← 差分
差分: 5件
```

## 対象ファイル

| ファイル | 操作 |
|---------|------|
| `src/bin/compare.rs` | 新規作成（~250行） |
| `Cargo.toml` | `[[bin]]` 追加 |

既存ファイルの変更: **なし**
