---
name: recalculate-test
description: daiun-salary プロジェクトの拘束時間管理表テスト→再計算ワークフロー。ドライバー指定で cargo test 実行 → ローカルサーバーの recalculate-driver API 呼び出し → DB値確認 → テストモック更新の一連フローを実行する。「再計算テスト」「recalculate test」「ドライバーの再計算」「テストして再集計」等で発動。
---

# Recalculate Test ワークフロー

daiun-salary の拘束時間管理表 CSV 比較テストとドライバー単位の再計算を行う。

## ワークフロー

### 1. テスト実行（差分確認）

```bash
cd /home/yhonda/rust/daiun-salary
cargo test test_compare_<driver_cd> -- --nocapture
```

### 2. サーバー起動確認

```bash
lsof -i :8080  # 起動中か確認
# 未起動なら:
source .env && cargo build && cargo run &
sleep 5
```

### 3. ドライバー単位の再計算

```bash
bash .claude/skills/recalculate-test/scripts/recalculate_driver.sh <driver_cd> [year] [month]
```

スクリプトが JWT 生成 → `POST /api/recalculate-driver` → DB値表示まで実行。

### 4. テストモック更新

DB出力値で `src/routes/restraint_report.rs` の `MockDwh` 配列を更新。

### 5. 再テスト → 差分0件を目指す

## 環境

- tenant_id: `85b9ef71-61c0-4a11-928e-c18c685648c2`
- DB: .env の DATABASE_URL（Supabase）
- JWT: .env の JWT_SECRET で PyJWT 生成
- サーバー: localhost:8080
- テスト: `src/routes/restraint_report.rs`
