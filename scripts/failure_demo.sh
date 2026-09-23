#!/usr/bin/env bash
# Kill-and-recover demo. Run from a clean start (scripts/fresh_start.sh).
#   scripts/failure_demo.sh taskmanager   # kill the Flink TaskManager for 20 s
#   scripts/failure_demo.sh postgres      # restart the Postgres container
# The simulator runs throughout; afterwards verify.sh must match. Slot samples are written to
# docs/failure-demo-<mode>-slot-trace.txt.
set -uo pipefail
cd "$(dirname "$0")/.."
MODE="${1:-taskmanager}"
CH() { curl -s "localhost:8123/?user=default&password=clickhouse" --data-binary "$1"; }
JID=$(curl -s localhost:8081/jobs/overview | python3 -c "import sys,json;print([j['jid'] for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING'][0])")
ck() { curl -s localhost:8081/jobs/$JID/checkpoints | python3 -c "
import sys,json
d=json.load(sys.stdin); c=d['latest']['completed']; r=d['latest'].get('restored')
print('latest completed checkpoint=%s, restored from=%s, counts=%s' % (c['id'] if c else None, r['id'] if r else None, d['counts']))"; }
st() { curl -s localhost:8081/jobs/$JID | python3 -c "import sys,json;print(json.load(sys.stdin)['state'])"; }
watch_state() { for _ in $(seq 1 "$1"); do echo "  $(date +%H:%M:%S) job=$(st)"; sleep "$2"; done; }

# Make replay duplicates observable: ClickHouse would otherwise collapse them before we can count
# them, both inside one insert batch (optimize_on_insert) and by background merges. Restored on exit.
CH "SYSTEM STOP MERGES cdc.resource_inventory" >/dev/null
CH "ALTER USER flink_sink SETTINGS optimize_on_insert = 0" >/dev/null
# sample_slot.sh runs for a fixed duration that may outlive this script (its 200s vs. this
# script's ~150s). Left alone, a second run started before the first's sampler exits would have
# two processes writing the same file at different offsets -- silent NUL-byte corruption, not a
# clean overwrite (this happened once; the committed trace file had to be regenerated). So the
# sampler's PID is tracked and explicitly killed on exit, success or failure, rather than trusted
# to finish on its own.
trap 'CH "SYSTEM START MERGES cdc.resource_inventory" >/dev/null; CH "ALTER USER flink_sink SETTINGS optimize_on_insert = 1" >/dev/null; kill "$SAMPLE_PID" 2>/dev/null' EXIT

TRACE="docs/failure-demo-$MODE-slot-trace.txt"
: > sim_ops.log
echo "### BEFORE"; ck
echo "postgres rows: $(docker exec cdc-postgres psql -U postgres -d inventory -tAc 'SELECT count(*) FROM resource_inventory')"
echo "clickhouse raw rows: $(CH 'SELECT count() FROM cdc.resource_inventory'), current view: $(CH 'SELECT count() FROM cdc.resource_inventory_current')"

scripts/simulate_changes.py --rate 20 --seconds 120 --seed 7 > /tmp/sim_out.txt 2>&1 &
SIM=$!
scripts/sample_slot.sh 200 5 > "$TRACE" 2>&1 &
SAMPLE_PID=$!
sleep 30
echo "### T+30s"; ck

if [ "$MODE" = "taskmanager" ]; then
  echo "### killing taskmanager at $(date +%H:%M:%S)"; docker compose kill taskmanager >/dev/null 2>&1
  watch_state 6 3
  echo "### restarting taskmanager at $(date +%H:%M:%S)"; docker compose up -d taskmanager >/dev/null 2>&1
  watch_state 10 3
else
  echo "### restarting postgres at $(date +%H:%M:%S)"; docker compose restart postgres >/dev/null 2>&1
  echo "postgres restart returned at $(date +%H:%M:%S)"
  watch_state 15 4
fi

wait $SIM; cat /tmp/sim_out.txt
OPS=$(grep -vc ERROR sim_ops.log); ERR=$(grep -c ERROR sim_ops.log)
echo "### AFTER: simulator ops logged=$OPS, failed batches=$ERR"; ck
echo "checkpoint offsets seen by the source:"; docker logs cdc-taskmanager 2>&1 | grep "Stream split offset on checkpoint" | tail -3 | sed 's/.*Stream split offset/  Stream split offset/' | cut -c1-120
echo "### VERIFY"; scripts/verify.sh --wait 120; RC=$?
RAW=$(CH "SELECT count() FROM cdc.resource_inventory")
DUP=$(CH "SELECT count() - uniqExact(resource_id, _version) FROM cdc.resource_inventory")
echo "raw rows: $RAW, exact duplicates (same resource_id and _version) from replay: $DUP"
echo "duplicate resource_ids: $(CH "SELECT count() FROM (SELECT resource_id, _version FROM cdc.resource_inventory GROUP BY resource_id, _version HAVING count() > 1)") distinct changes were written more than once"
echo "current view: $(CH 'SELECT count() FROM cdc.resource_inventory_current') rows, postgres: $(docker exec cdc-postgres psql -U postgres -d inventory -tAc 'SELECT count(*) FROM resource_inventory')"
echo "verify exit code: $RC"
exit $RC
