#!/usr/bin/env bash
# Usage: generate_mock.sh <driver_cd> [year] [month]
# Generates MockDwh Rust code from DB values
set -euo pipefail

DRIVER_CD="${1:?Usage: generate_mock.sh <driver_cd> [year] [month]}"
YEAR="${2:-2026}"
MONTH="${3:-2}"

cd /home/yhonda/rust/daiun-salary
source .env

TENANT_ID="85b9ef71-61c0-4a11-928e-c18c685648c2"
DB_PASS=$(echo "$DATABASE_URL" | sed 's|.*://[^:]*:\([^@]*\)@.*|\1|')
DB_CONN=$(echo "$DATABASE_URL" | sed 's|://[^:]*:[^@]*@|://postgres@|')

MONTH_START="${YEAR}-$(printf '%02d' $MONTH)-01"
MONTH_END="${YEAR}-$(printf '%02d' $MONTH)-28"

PGPASSWORD="$DB_PASS" psql "$DB_CONN" -t -A -F'|' -c "
SELECT EXTRACT(DAY FROM dwh.work_date)::int,
       COALESCE(to_char(MIN(o.departure_at), 'FMHH24:MI'), ''),
       COALESCE(to_char(MAX(dws.end_at), 'FMHH24:MI'), ''),
       dwh.drive_minutes, dwh.overlap_drive_minutes,
       dwh.cargo_minutes, dwh.overlap_cargo_minutes,
       dwh.total_work_minutes, dwh.overlap_restraint_minutes,
       dwh.late_night_minutes, COALESCE(dwh.ot_late_night_minutes, 0)
FROM daily_work_hours dwh
JOIN drivers d ON dwh.driver_id = d.id
LEFT JOIN operations o ON o.driver_id = d.id AND o.tenant_id = dwh.tenant_id AND o.unko_no = ANY(dwh.unko_nos)
LEFT JOIN daily_work_segments dws ON dws.driver_id = d.id AND dws.tenant_id = dwh.tenant_id AND dws.work_date = dwh.work_date
WHERE d.driver_cd = '$DRIVER_CD' AND dwh.work_date >= '$MONTH_START' AND dwh.work_date <= '$MONTH_END'
GROUP BY dwh.work_date, dwh.drive_minutes, dwh.overlap_drive_minutes, dwh.cargo_minutes, dwh.overlap_cargo_minutes, dwh.total_work_minutes, dwh.overlap_restraint_minutes, dwh.late_night_minutes, dwh.ot_late_night_minutes
ORDER BY dwh.work_date;
" | while IFS='|' read day st et dr odr ca oca re ore ln oln; do
  printf '            MockDwh { day: %-2s, start_time: "%-5s", end_time: "%-5s", drive: %4s, overlap_drive: %3s, cargo: %3s, overlap_cargo: %s, restraint: %4s, overlap_restraint: %3s, late_night: %3s, ot_late_night: %s },\n' \
    "$day" "$st" "$et" "$dr" "$odr" "$ca" "$oca" "$re" "$ore" "$ln" "$oln"
done
