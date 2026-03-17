#!/bin/bash
set -euo pipefail

# 拘束時間管理システム test → deploy 統合スクリプト
#
# Usage:
#   ./test_and_deploy.sh                    # テストのみ（デプロイしない）
#   ./test_and_deploy.sh --deploy           # テスト通過後にデプロイ
#   ./test_and_deploy.sh --skip-compare     # CSV比較をスキップ
#   ./test_and_deploy.sh --skip-compare --deploy

DEPLOY=false
SKIP_COMPARE=false

for arg in "$@"; do
    case $arg in
        --deploy) DEPLOY=true ;;
        --skip-compare) SKIP_COMPARE=true ;;
        --help|-h)
            echo "Usage: $0 [--deploy] [--skip-compare]"
            echo "  --deploy         テスト通過後にデプロイを実行"
            echo "  --skip-compare   CSV比較をスキップ"
            exit 0
            ;;
    esac
done

echo "=== Step 1/4: cargo fmt --check ==="
cargo fmt --check
echo "  OK"

echo ""
echo "=== Step 2/4: cargo clippy ==="
cargo clippy 2>&1 | tail -5
echo "  OK"

echo ""
echo "=== Step 3/4: cargo test (restraint_report) ==="
cargo test restraint_report 2>&1 | tail -10
echo "  OK"

if [ "$SKIP_COMPARE" = false ]; then
    echo ""
    echo "=== Step 4/4: CSV比較 (ZIP→計算→参照CSV) ==="
    cargo run --bin compare -- \
        "test_data/csvdata-202602-1018-1021-1026.zip" \
        "test_data/拘束時間管理表_202602-1018-1021-1026.csv" \
        --json | tail -5
    echo "  OK（差分なし）"
else
    echo ""
    echo "=== Step 4/4: CSV比較 — スキップ ==="
fi

echo ""
echo "========================================="
echo "  全チェック通過"
echo "========================================="

if [ "$DEPLOY" = true ]; then
    echo ""
    echo "=== デプロイ開始 ==="
    ./deploy.sh
else
    echo ""
    echo "(--deploy を指定するとデプロイを実行します)"
fi
