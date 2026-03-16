#!/usr/bin/env bash
# Usage: recalculate_and_test.sh [year] [month]
# 1021/1026を再計算→モック自動書き換え→cargo test実行
set -euo pipefail

YEAR="${1:-2026}"
MONTH="${2:-2}"

cd /home/yhonda/rust/daiun-salary
source .env

TENANT_ID="85b9ef71-61c0-4a11-928e-c18c685648c2"
DB_PASS=$(echo "$DATABASE_URL" | sed 's|.*://[^:]*:\([^@]*\)@.*|\1|')
DB_CONN=$(echo "$DATABASE_URL" | sed 's|://[^:]*:[^@]*@|://postgres@|')
EP="https://daiun-salary-747065218280.asia-northeast1.run.app"
RS_FILE="src/routes/restraint_report.rs"
SCRIPT_DIR=".claude/skills/recalculate-test/scripts"

# 1. 再計算
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

# 2. モック自動書き換え
for DC in 1021 1026; do
  echo "==> Updating mock for $DC..."
  MOCK=$(bash "$SCRIPT_DIR/generate_mock.sh" "$DC" "$YEAR" "$MONTH")
  if [ -z "$MOCK" ]; then
    echo "ERROR: generate_mock.sh returned empty for $DC"
    exit 1
  fi
  # マーカー間を置換
  START_MARKER="// MOCK_${DC}_START"
  END_MARKER="// MOCK_${DC}_END"
  python3 -c "
import re, sys
with open('$RS_FILE', 'r') as f:
    content = f.read()
mock_data = '''$MOCK'''
pattern = re.compile(
    r'(\\s*' + re.escape('$START_MARKER') + r'\\n).*?(\\s*' + re.escape('$END_MARKER') + r')',
    re.DOTALL
)
new_content = pattern.sub(r'\\1' + mock_data + r'\\n\\2', content)
if new_content == content:
    print(f'WARNING: marker not found for $DC', file=sys.stderr)
    sys.exit(1)
with open('$RS_FILE', 'w') as f:
    f.write(new_content)
print(f'  Updated $DC mock ({mock_data.count(chr(10))} lines)')
"
done

# 3. テスト実行
echo ""
echo "==> Running tests..."
cargo test restraint_report -- --nocapture 2>&1 | grep -E "(diffs|FAILED|test result)"
