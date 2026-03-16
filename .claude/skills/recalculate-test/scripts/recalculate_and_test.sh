#!/usr/bin/env bash
# Usage: recalculate_and_test.sh [year] [month]
# 1021/1026を再計算→モック生成→cargo test実行
set -euo pipefail

YEAR="${1:-2026}"
MONTH="${2:-2}"

cd /home/yhonda/rust/daiun-salary
source .env

TENANT_ID="85b9ef71-61c0-4a11-928e-c18c685648c2"
DB_PASS=$(echo "$DATABASE_URL" | sed 's|.*://[^:]*:\([^@]*\)@.*|\1|')
DB_CONN=$(echo "$DATABASE_URL" | sed 's|://[^:]*:[^@]*@|://postgres@|')
EP="https://daiun-salary-747065218280.asia-northeast1.run.app"

for DC in 1021 1026; do
  DRIVER_ID=$(PGPASSWORD="$DB_PASS" psql -t "$DB_CONN" -c \
    "SELECT id FROM drivers WHERE driver_cd = '$DC' AND tenant_id = '$TENANT_ID';" | tr -d ' ')
  TOKEN=$(python3 -c "
import jwt, time, uuid
print(jwt.encode({
    'sub': str(uuid.uuid4()),
    'email': 'local@test.com',
    'name': 'Local Test',
    'tenant_id': '$TENANT_ID',
    'role': 'admin',
    'iat': int(time.time()),
    'exp': int(time.time()) + 86400,
}, '$JWT_SECRET', algorithm='HS256'))
")
  echo "==> Recalculating $DC..."
  timeout 300 curl -s -N -X POST \
    -H "Authorization: Bearer $TOKEN" \
    "$EP/api/recalculate-driver?year=$YEAR&month=$MONTH&driver_id=$DRIVER_ID" 2>&1 | grep '"done"'
done

echo ""
echo "==> Generating mocks..."
for DC in 1021 1026; do
  echo "--- $DC ---"
  bash .claude/skills/recalculate-test/scripts/generate_mock.sh "$DC" "$YEAR" "$MONTH"
done

echo ""
echo "==> Running tests..."
cargo test restraint_report -- --nocapture 2>&1 | grep -E "(diffs|FAILED|test result)"
