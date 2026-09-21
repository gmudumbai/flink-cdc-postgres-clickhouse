#!/usr/bin/env bash
# Clean slate: recreate the stack (no replication slot, no ClickHouse data), submit the job,
# wait until the snapshot has landed. Usage: scripts/fresh_start.sh
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose down >/dev/null 2>&1
docker compose up -d >/dev/null 2>&1
echo -n "waiting for services"; until [ "$(docker compose ps --format '{{.Health}}' | grep -c healthy)" -ge 3 ]; do echo -n .; sleep 3; done; echo
until curl -sf localhost:8081/overview | grep -q '"taskmanagers":1'; do sleep 2; done
docker exec cdc-jobmanager flink run -d /opt/flink/usrlib/cdc-job.jar 2>&1 | grep submitted
scripts/verify.sh --wait 90 | head -1
