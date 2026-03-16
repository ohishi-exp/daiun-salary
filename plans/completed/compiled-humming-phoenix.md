# CSV差分解消: KUDGIVTイベント単位で検証・修正

## Context
日帰属変更は完了したが、まだ96件の差分がある。同じ分析を繰り返すのではなく、R2に運行NO単位で保存されているKUDGIVTデータを直接ダウンロードし、CSVの値と1つ1つ突き合わせて差分原因を特定・修正する。

## R2ストレージ構造
```
s3://ohishi-dtako/{tenant_id}/unko/{unko_no}/
  ├── KUDGIVT.csv   ← イベントデータ（運転/荷役/休憩/休息）
  ├── KUDGURI.csv   ← 運行メタデータ
  ├── KUDGFRY.csv
  ├── KUDGFUL.csv
  └── SOKUDODATA.csv
```

## Step 1. KUDGIVTデータのダウンロード

1026の全運行NOのKUDGIVT.csvをダウンロードして `test_data/kudgivt/1026_2026-02/` に保存：

```bash
# 1026の全unko_noを取得（DBから）
# 各unko_noのKUDGIVT.csvをR2から取得
# test_data/kudgivt/1026_2026-02/{unko_no}.csv として保存
```

対象unko_no（DBから取得済み）:
- 2602020117450000001867, 2602022317430000001867（2/2 2運行）
- 2602040120440000001867（2/4）
- 2602050111340000001867, 2602052318070000001867（2/5 2運行）
- 2602070126230000001867（2/7 シンプル1運行 → 検証の起点）
- ...他

## Step 2. シンプルな1運行（2/7）で突き合わせ

2/7 は1運行のみ（dep 1:26, ret 15:48）。CSV値:
```
始業=1:26, 終業=15:48, 運転=7:20, 荷役=2:46, 休憩=3:01
小計=13:07, 合計=13:07, 実働=10:06, 時間外=2:06, 深夜=3:24
```

システム値（DB）:
```
drive=440, cargo=166, restraint=802, late_night=214
→ 小計=13:22(+15min), 深夜=3:34(+10min)
```

KUDGIVTイベントを展開して:
- [ ] 全イベントの event_cd, start_at, duration を一覧化
- [ ] 110(運転), 202/203(荷役), 301(休憩), 302(休息) の合計を算出
- [ ] CSV値（7:20, 2:46, 3:01）と一致するか確認
- [ ] 拘束時間の計算方法を特定: wall-clock-60min vs event合算 vs 別の方法
- [ ] 深夜時間の計算方法を特定: 全拘束中22-05 vs event中22-05 vs 別の方法

## Step 3. 差分原因の特定と修正

Step 2の結果に基づいて:
- [ ] `calculate_daily_hours` の各計算を修正
- [ ] 2運行日（2/2, 2/5）でも検証
- [ ] 重複(overlap)計算の検証
- [ ] モックデータ更新 → テスト実行

## Step 0. CLAUDE.mdにR2ストレージ構造を追記

R2の `unko/{unko_no}/` 構造をCLAUDE.mdに追記して今後の参照用にする。

## 対象ファイル
- `test_data/kudgivt/1026_2026-02/` — ダウンロードしたイベントデータ
- `src/routes/upload.rs` — calculate_daily_hours の修正
- `src/routes/restraint_report.rs` — モックデータ更新

## 検証
```bash
cargo test test_compare_1026 -- --nocapture
# 差分0件を目指す
```
