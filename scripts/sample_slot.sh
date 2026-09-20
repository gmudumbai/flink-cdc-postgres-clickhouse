#!/usr/bin/env bash
# Prints one compact slot sample every 15s for a duration. Usage: sample_slot.sh SECONDS
set -euo pipefail
end=$((SECONDS + ${1:-120}))
while [ $SECONDS -lt $end ]; do
  printf '%s  ' "$(date +%H:%M:%S)"
  docker exec cdc-postgres psql -U postgres -d inventory -tAc \
   "SELECT 'active='||active||'  retained_wal='||pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))||'  unconfirmed='||pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots WHERE slot_name='flink_slot'"
  sleep 15
done
