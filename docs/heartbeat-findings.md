# Heartbeat experiment: findings (Phase 1)

Stack: Postgres 16, Flink 1.20.5, Flink CDC `3.6.0-1.20` (Debezium 1.9.8), pgoutput.
Load: `scripts/churn_other_table.py`, 300 rows/s into `unrelated_writes` (not in the publication),
`resource_inventory` untouched. Samples every 15 s via `scripts/sample_slot.sh`.

| Run | Heartbeat | Checkpoint | Result |
|---|---|---|---|
| A  (`heartbeat-off.txt`) | off | 10 s | `retained_wal` bounded at ~1-3 MB, slot fully caught up at the end |
| A2 (`heartbeat-off-slow-checkpoints.txt`) | off | 10 min, `--startup latest` | same, bounded at ~1-3 MB |
| B  (`heartbeat-on.txt`) | 5 s | 10 s | same, bounded at ~1-3 MB |

## What this shows

The failure the plan predicts (retained WAL climbing steadily while the consumer is healthy and the
published table is quiet) did **not** reproduce on this stack. An active consumer kept the slot
advancing past ~14 MB of WAL from an unpublished table with the heartbeat disabled, and slowing
checkpoints to 10 minutes did not change that, so confirmation is not checkpoint-driven.
The mechanism is not established here.

The heartbeat also did not visibly fire: `cdc_heartbeat.ts` was updated only twice, in the first
job, and never in runs A, A2 or B. So run B is not a valid "heartbeat on" comparison, and the
property `heartbeat.action.query` is not confirmed to be taking effect through the incremental
source. Unresolved; needs investigating before relying on it (Flink CDC also exposes its own
`heartbeat.interval` builder option, untried).

## Side findings

- The incremental snapshot only hands off to the stream reader after a checkpoint completes. A very
  long checkpoint interval leaves the job in the snapshot phase with the slot `active=false`.
- The checkpoint volume must be writable by the `flink` user (`checkpoint-perms` service in Compose).

## Update after upgrading to Flink 2.2.1 / Flink CDC 3.6.0-2.2 / Java 21

On the upgraded stack the heartbeat **does fire**: `cdc_heartbeat.ts` advanced every ~5 s
with the same job code and the same `heartbeat.action.query`. The experiments above ran on
Flink 1.20 and have not been repeated; re-running them on 2.2 is still open.
