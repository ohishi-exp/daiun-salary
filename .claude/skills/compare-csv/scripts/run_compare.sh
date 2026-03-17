#!/bin/bash
# 拘束時間管理表 CSV比較 CLIラッパー
# Usage: run_compare.sh [csv1] [csv2] [-d driver_cd]
#
# Examples:
#   run_compare.sh                                    # デフォルト: test_data内の参照CSV表示
#   run_compare.sh reference.csv system.csv           # 2ファイル比較
#   run_compare.sh reference.csv system.csv -d 1026   # ドライバー指定

set -euo pipefail
cd /home/yhonda/rust/daiun-salary

DEFAULT_CSV="test_data/拘束時間管理表_202602-1021-1026.csv"

if [ $# -eq 0 ]; then
    echo "=== デフォルト: ${DEFAULT_CSV} のサマリー ==="
    cargo run --bin compare -- "$DEFAULT_CSV" 2>&1
else
    cargo run --bin compare -- "$@" 2>&1
fi
