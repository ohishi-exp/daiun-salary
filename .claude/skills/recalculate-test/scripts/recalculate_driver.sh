#!/usr/bin/env bash
# Usage: recalculate_driver.sh <driver_cd> [year] [month]
# Example: recalculate_driver.sh 1026 2026 2
set -euo pipefail

DRIVER_CD="${1:?Usage: recalculate_driver.sh <driver_cd> [year] [month]}"
YEAR="${2:-2026}"
MONTH="${3:-2}"

cd /home/yhonda/rust/daiun-salary
source .env

TENANT_ID="85b9ef71-61c0-4a11-928e-c18c685648c2"
DB_PASS=$(echo "$DATABASE_URL" | sed 's|.*://[^:]*:\([^@]*\)@.*|\1|')
DB_CONN=$(echo "$DATABASE_URL" | sed 's|://[^:]*:[^@]*@|://postgres@|')

# 1. Get driver UUID
DRIVER_ID=$(PGPASSWORD="$DB_PASS" psql -t "$DB_CONN" -c \
  "SELECT id FROM drivers WHERE driver_cd = '$DRIVER_CD' AND tenant_id = '$TENANT_ID';" | tr -d ' ')

if [ -z "$DRIVER_ID" ]; then
  echo "ERROR: driver_cd=$DRIVER_CD not found"
  exit 1
fi
echo "driver_cd=$DRIVER_CD -> driver_id=$DRIVER_ID"

# 2. Generate JWT
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

# 3. Check server
if ! curl -s -o /dev/null -w '' http://localhost:8080/health 2>/dev/null; then
  echo "Server not running on :8080. Starting..."
  cargo build 2>&1 | tail -2
  source .env && cargo run &
  sleep 5
fi

# 4. Call recalculate-driver
echo "Recalculating driver $DRIVER_CD for $YEAR-$MONTH..."
timeout 300 curl -s -N -X POST \
  -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8080/api/recalculate-driver?year=$YEAR&month=$MONTH&driver_id=$DRIVER_ID" 2>&1

echo ""

# 5. Show updated DB values
echo "=== Updated DB values ==="
PGPASSWORD="$DB_PASS" psql "$DB_CONN" -c "
SELECT EXTRACT(DAY FROM dwh.work_date)::int as day,
       dwh.drive_minutes, dwh.overlap_drive_minutes,
       dwh.cargo_minutes, dwh.overlap_cargo_minutes,
       dwh.total_work_minutes as restraint, dwh.overlap_restraint_minutes,
       dwh.late_night_minutes, dwh.ot_late_night_minutes
FROM daily_work_hours dwh
JOIN drivers d ON dwh.driver_id = d.id
WHERE d.driver_cd = '$DRIVER_CD'
  AND dwh.work_date >= '$YEAR-$(printf '%02d' $MONTH)-01'
  AND dwh.work_date <= '$YEAR-$(printf '%02d' $MONTH)-28'
ORDER BY dwh.work_date;
"
