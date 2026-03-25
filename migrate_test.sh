#!/bin/bash
set -euo pipefail

# マイグレーション検証スクリプト
# ローカルのPostgresにマイグレーションを適用し、splinter(Supabase linter)でチェックする
#
# Usage:
#   ./migrate_test.sh              # ローカルDBでmigrate→lint→削除
#   ./migrate_test.sh --db-url URL # 指定DBに対してlintのみ実行

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONTAINER_NAME="migrate-test-pg"
LOCAL_PORT=54321
LOCAL_DB_URL="postgresql://postgres:test@localhost:${LOCAL_PORT}/postgres?sslmode=disable"
LINT_ONLY=false
TARGET_DB_URL=""

for arg in "$@"; do
    case $arg in
        --db-url=*)
            TARGET_DB_URL="${arg#*=}"
            LINT_ONLY=true
            ;;
        --help|-h)
            echo "Usage: $0 [--db-url=URL]"
            echo "  (no args)        ローカルPostgresを起動→migrate→lint→削除"
            echo "  --db-url=URL     指定DBに対してlintのみ実行"
            exit 0
            ;;
    esac
done

run_splinter() {
    local db_url="$1"
    local result
    # セキュリティ関連のlintを抽出（{SECURITY}カテゴリタグでフィルタ）
    result=$(psql "$db_url" -t -f "$SCRIPT_DIR/splinter.sql" 2>/dev/null \
        | grep '{SECURITY}' || true)
    if [ -n "$result" ]; then
        echo "$result"
        return 1
    fi
    return 0
}

cleanup() {
    if [ "$LINT_ONLY" = false ]; then
        echo "==> Cleaning up..."
        docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
    fi
}
trap cleanup EXIT

if [ "$LINT_ONLY" = true ]; then
    echo "=== Splinter lint (target DB) ==="
    run_splinter "$TARGET_DB_URL"
    echo "  OK"
    exit 0
fi

echo "=== Step 1: Start local Postgres ==="
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
docker run -d --name "$CONTAINER_NAME" \
    -e POSTGRES_PASSWORD=test \
    -p "${LOCAL_PORT}:5432" \
    postgres:16 > /dev/null
echo "  Waiting for Postgres to be ready..."
for i in $(seq 1 30); do
    if pg_isready -h localhost -p "$LOCAL_PORT" -q 2>/dev/null; then
        break
    fi
    sleep 1
done
if ! pg_isready -h localhost -p "$LOCAL_PORT" -q 2>/dev/null; then
    echo "ERROR: Postgres failed to start"
    exit 1
fi
# Supabase互換の初期設定（ロール + PostgREST設定）
psql "$LOCAL_DB_URL" -q <<'SQL'
CREATE ROLE anon NOLOGIN;
CREATE ROLE authenticated NOLOGIN;
CREATE ROLE service_role NOLOGIN;
GRANT USAGE ON SCHEMA public TO anon, authenticated, service_role;
-- splinterがpgrst.db_schemasを参照するため、カスタムパラメータとして登録
ALTER DATABASE postgres SET pgrst.db_schemas = 'public';
SQL
echo "  OK"

echo "=== Step 2: Apply migrations ==="
sqlx migrate run --database-url "$LOCAL_DB_URL"
echo "  OK ($(ls migrations/*.sql | wc -l) migration files)"

echo "=== Step 3: Splinter lint ==="
run_splinter "$LOCAL_DB_URL"
echo "  OK"

echo ""
echo "========================================="
echo "  All checks passed"
echo "========================================="
