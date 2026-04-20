#!/bin/bash
# daiun-salary: 共通テスト+デプロイスクリプトを呼び出す
# --skip-compare は --skip-extra にマッピング
ARGS=()
for arg in "$@"; do
    case $arg in
        --skip-compare) ARGS+=("--skip-extra") ;;
        *) ARGS+=("$arg") ;;
    esac
done
exec bash ~/.claude/skills/migrate-test/scripts/test_and_deploy.sh "${ARGS[@]}"
