#!/usr/bin/env bash
# Replication slot health for the CDC pipeline.
#   active        is a consumer attached
#   retained_wal  WAL Postgres cannot recycle (disk risk)
#   unconfirmed   how far behind the consumer is
# Usage: scripts/slot_health.sh [--watch]
set -euo pipefail

CONTAINER="${PG_CONTAINER:-cdc-postgres}"

SQL="
SELECT
  slot_name,
  active,
  active_pid,
  pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))         AS retained_wal,
  pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS unconfirmed,
  restart_lsn,
  confirmed_flush_lsn
FROM pg_replication_slots;
"

run_once() {
  local count
  count=$(docker exec "$CONTAINER" psql -U postgres -d inventory -tAc "SELECT count(*) FROM pg_replication_slots")
  if [ "$count" = "0" ]; then
    echo "no replication slots"
    return 0
  fi
  docker exec "$CONTAINER" psql -U postgres -d inventory -x -c "$SQL"
}

if [ "${1:-}" = "--watch" ]; then
  while true; do
    clear
    date
    run_once
    sleep 2
  done
else
  run_once
fi
