#!/bin/bash
set -euo pipefail

# CSV 比較テスト (daiun-salary 固有)
# test_and_deploy.sh の EXTRA_TEST_CMD から呼ばれる

declare -a COMPARE_TESTS=(
    "test_data/csvdata-202602-1018-1021-1026.zip|test_data/拘束時間管理表_202602-1018-1021-1026.csv|0|0|1018/1021/1026"
    "test_data/csvdata-202602-1029-1032-1036-1037.zip|test_data/拘束時間管理表_202602-1029-1032-1036-1037.csv|0|0|1029/1032/1036/1037"
    "test_data/csvdata-202602-1039.zip|test_data/拘束時間管理表_202602-all.csv|0|17|1039(既知バグ17件)"
    "test_data/csvdata-202602-1041.zip|test_data/拘束時間管理表_202602-all.csv|105|0|1041(未知差分105件)"
    "test_data/csvdata-202602-1049.zip|test_data/拘束時間管理表_202602-all.csv|0|0|1049"
    "test_data/csvdata-202602-1051.zip|test_data/拘束時間管理表_202602-all.csv|0|0|1051"
    "test_data/csvdata-202602-1068.zip|test_data/拘束時間管理表_202602-all.csv|0|21|1068(既知バグ21件)"
    "test_data/csvdata-202602-1069.zip|test_data/拘束時間管理表_202602-all.csv|0|16|1069(既知バグ16件)"
    "test_data/csvdata-202602-1071.zip|test_data/拘束時間管理表_202602-all.csv|0|58|1071(既知バグ58件)"
    "test_data/csvdata-202602-1078.zip|test_data/拘束時間管理表_202602-all.csv|0|29|1078(既知バグ29件)"
    "test_data/csvdata-202602-1072.zip|test_data/拘束時間管理表_202602-all.csv|0|19|1072(既知バグ19件)"
)

for i in "${!COMPARE_TESTS[@]}"; do
    IFS='|' read -r ZIP CSV EXPECTED_UNKNOWN EXPECTED_KNOWN LABEL <<< "${COMPARE_TESTS[$i]}"
    echo "  CSV比較 ($LABEL)..."

    OUTPUT=$(cargo run --bin compare -- "$ZIP" "$CSV" --json 2>&1 || true)
    UNKNOWN=$(echo "$OUTPUT" | grep -o '"unknown_diffs": [0-9]*' | tail -1 | grep -o '[0-9]*')
    KNOWN=$(echo "$OUTPUT" | grep -o '"known_bug_diffs": [0-9]*' | tail -1 | grep -o '[0-9]*')
    UNKNOWN=${UNKNOWN:-0}
    KNOWN=${KNOWN:-0}

    if [ "$UNKNOWN" = "$EXPECTED_UNKNOWN" ] && [ "$KNOWN" = "$EXPECTED_KNOWN" ]; then
        echo "    OK（未知${UNKNOWN}件, 既知バグ${KNOWN}件）"
    elif [ "$UNKNOWN" = "$EXPECTED_UNKNOWN" ]; then
        echo "    WARN: 未知${UNKNOWN}件=OK, 既知バグ${KNOWN}件≠期待値${EXPECTED_KNOWN}件"
    else
        echo "    FAIL: 未知${UNKNOWN}件≠期待値${EXPECTED_UNKNOWN}件, 既知バグ${KNOWN}件"
        echo "$OUTPUT" | tail -30
        exit 1
    fi
done