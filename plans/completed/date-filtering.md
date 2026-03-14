# 日付フィルタリング実装計画

## Context

カレンダーUIで日付を選択してスクレイプしても、dtako-scraper は rdoSelect0（全選択）で全期間データをDLしている。rdoSelect1（日付範囲指定）は Python では `type` 入力で動作確認済み。Rust 側では `evaluate()` で値を直接セットしていたため ASP.NET ViewState が同期せず失敗していた。

**修正方針:** `download.rs` を rdoSelect1 + `Element::type_str()` でキーストローク入力に変更し、指定日付範囲のみDLする。

## 変更箇所

### 1. dtako-scraper: `download.rs` — rdoSelect1 + type 入力に変更

**ファイル:** `/home/yhonda/rust/dtako-scraper/src/scraper/download.rs`

- [x] `_start_date` → `start_date`、`_end_date` → `end_date` にリネーム（アンダースコア削除）
- [x] 日付文字列を年(2桁)/月(2桁)/日(2桁)にパース（`YYYY-MM-DD` → `YY`, `MM`, `DD`）
- [x] rdoSelect0 の全選択ロジック（L56-97: btnSelectAll クリック）を削除
- [x] rdoSelect1 モードに切り替え + `type_str` で日付入力
- [x] btnCsv クリック部分はそのまま維持
- [x] コメントを更新
- [x] cargo build 成功

### 日付フィールド ID 一覧
| フィールド | セレクタ |
|-----------|---------|
| 開始年 | `#MainContent_ucStartDate_txtYear` |
| 開始月 | `#MainContent_ucStartDate_txtMonth` |
| 開始日 | `#MainContent_ucStartDate_txtDay` |
| 終了年 | `#MainContent_ucEndDate_txtYear` |
| 終了月 | `#MainContent_ucEndDate_txtMonth` |
| 終了日 | `#MainContent_ucEndDate_txtDay` |

### 日付パース
`start_date` = `"2026-03-01"` → Year=`"26"`, Month=`"03"`, Day=`"01"`

### 注意点
- `type_str` の前に既存値をクリアする必要あり（`Ctrl+A` → 上書き、または `triple_click` でセレクト）
  - `find_element` → `click()` → JS で `el.value = ''` → `type_str()`
  - または `click()` → `press_key("Control+a")` → `type_str()` で上書き

## 変更不要なもの

- `mod.rs`: `start_date`/`end_date` は既に `download_csv()` に渡されている
- `upload.rs` (dtako-scraper): 変更不要
- `daiun-salary` 側: 変更不要（全データではなく日付範囲分のみ受け取るため）
- フロントエンド: 既に `start_date`/`end_date` を送信済み
- DB マイグレーション: 不要

## 検証方法

1. `cargo build` でコンパイル通ること
2. デプロイ後、カレンダーUIから日付指定（例: 3/1〜3/7）でスクレイプ
3. DLされた ZIP に指定範囲のデータのみ含まれることを確認
4. operations テーブルに指定範囲のみ追加されることを確認
