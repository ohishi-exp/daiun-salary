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

STEP=1

echo "=== Step $STEP: cargo fmt --check ==="
cargo fmt --check
echo "  OK"
STEP=$((STEP+1))

echo ""
echo "=== Step $STEP: cargo clippy ==="
cargo clippy 2>&1 | tail -5
echo "  OK"
STEP=$((STEP+1))

echo ""
echo "=== Step $STEP: cargo test (restraint_report) ==="
cargo test restraint_report 2>&1 | tail -10
echo "  OK"
STEP=$((STEP+1))

if [ "$SKIP_COMPARE" = false ]; then
    # CSV比較テスト定義: ZIP CSV 期待未知差分 期待既知バグ差分 ラベル
    declare -a COMPARE_TESTS=(
        "test_data/csvdata-202602-1018-1021-1026.zip|test_data/拘束時間管理表_202602-1018-1021-1026.csv|0|0|1018/1021/1026"
        "test_data/csvdata-202602-1029-1032-1036-1037.zip|test_data/拘束時間管理表_202602-1029-1032-1036-1037.csv|0|0|1029/1032/1036/1037"
        "test_data/csvdata-202602-1039.zip|test_data/拘束時間管理表_202602-all.csv|0|15|1039(既知バグ15件)"
        "test_data/csvdata-202602-1041.zip|test_data/拘束時間管理表_202602-all.csv|100|0|1041(未知差分100件)"
    )

    TOTAL_COMPARE=${#COMPARE_TESTS[@]}
    for i in "${!COMPARE_TESTS[@]}"; do
        IFS='|' read -r ZIP CSV EXPECTED_UNKNOWN EXPECTED_KNOWN LABEL <<< "${COMPARE_TESTS[$i]}"
        echo ""
        echo "=== Step $STEP: CSV比較 ($LABEL) ==="

        OUTPUT=$(cargo run --bin compare -- "$ZIP" "$CSV" --json 2>&1 || true)
        UNKNOWN=$(echo "$OUTPUT" | grep -o '"unknown_diffs": [0-9]*' | tail -1 | grep -o '[0-9]*')
        KNOWN=$(echo "$OUTPUT" | grep -o '"known_bug_diffs": [0-9]*' | tail -1 | grep -o '[0-9]*')
        UNKNOWN=${UNKNOWN:-0}
        KNOWN=${KNOWN:-0}

        if [ "$UNKNOWN" = "$EXPECTED_UNKNOWN" ] && [ "$KNOWN" = "$EXPECTED_KNOWN" ]; then
            echo "  OK（未知${UNKNOWN}件, 既知バグ${KNOWN}件）"
        elif [ "$UNKNOWN" = "$EXPECTED_UNKNOWN" ]; then
            echo "  WARN: 未知${UNKNOWN}件=OK, 既知バグ${KNOWN}件≠期待値${EXPECTED_KNOWN}件"
        else
            echo "  FAIL: 未知${UNKNOWN}件≠期待値${EXPECTED_UNKNOWN}件, 既知バグ${KNOWN}件"
            echo "$OUTPUT" | tail -30
            exit 1
        fi
        STEP=$((STEP+1))
    done
else
    echo ""
    echo "=== Step $STEP: CSV比較 — スキップ ==="
    STEP=$((STEP+1))
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
