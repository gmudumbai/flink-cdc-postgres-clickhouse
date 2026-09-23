# Failure demo: kill it, watch the slot, prove the tables match

Everything here is scripted and repeatable. Each run starts from a clean stack, applies ~20
changes/second to Postgres for 2 minutes (inserts, updates, deletes, some NULLs), injects a
failure 30 seconds in, and finishes with `verify.sh`.

```bash
scripts/fresh_start.sh                      # recreate stack, submit job, wait for the snapshot to match
scripts/failure_demo.sh taskmanager         # kill the TaskManager for ~20 s
scripts/fresh_start.sh
scripts/failure_demo.sh postgres            # restart the Postgres container
```

Raw output: [taskmanager](failure-demo-taskmanager-output.txt), [postgres](failure-demo-postgres-output.txt).
Slot samples every 5 s: [taskmanager](failure-demo-taskmanager-slot-trace.txt), [postgres](failure-demo-postgres-slot-trace.txt).

## What is configured (Flink job)
Exactly-once checkpointing every 10 s (min pause 3 s) to the shared volume, checkpoints retained
on cancel, fixed-delay restart (3 attempts, 5 s apart). The checkpoint stores the source's WAL
position (LSN) and the sink's buffered requests.

## Result 1: TaskManager killed at 10:39:24, restarted at 10:39:43

| | |
|---|---|
| Restored from | **checkpoint 4**, the last one completed before the kill |
| Checkpoints during the outage | 2 failed, then normal |
| Simulator | 2400 operations, 0 failed batches |
| `verify.sh` | **exit 0**, 462 rows on both sides, every per-team and per-state aggregate identical |
| Replay duplicates | **40** changes were written to ClickHouse twice |
| `_current` view | still exactly correct (462 = Postgres) |

Slot behaviour (`docs/failure-demo-taskmanager-slot-trace.txt`):

```
10:39:20  active=true   retained_wal=572 kB  restart=0/1BD0648
10:39:25  active=false  retained_wal=47 kB   restart=0/1C5AFF8   <- TaskManager killed
10:39:40  active=false  retained_wal=146 kB  restart=0/1C5AFF8   <- WAL grows, restart_lsn frozen
10:39:45  active=false  retained_wal=173 kB  restart=0/1C5AFF8
10:39:56  active=true   retained_wal=242 kB  restart=0/1C5AFF8   <- reconnected
10:40:01  active=true   retained_wal=179 kB  restart=0/1C72728   <- drained, restart_lsn advances after a checkpoint
```

While the consumer was gone, Postgres kept every WAL byte after `restart_lsn` (it did not jump
past unprocessed data). After reconnecting, the job replayed from its checkpoint and the retained
WAL shrank once a checkpoint completed. This is what a replication slot is for.

## Result 2: Postgres container restarted at 10:42:05 (under load)

| | |
|---|---|
| Job | went `RESTARTING` once, back to `RUNNING` about 15 s later (1 of 3 restart attempts used) |
| Restored from | **checkpoint 4** |
| Simulator | 2260 operations logged, 6 batches failed while Postgres was down (expected; logged, never applied) |
| `verify.sh` | **exit 0**, 459 rows on both sides |
| Replay duplicates | **80** |

The replication slot lives in Postgres's data directory, so it survived the restart.

## Observations worth knowing

- **The Flink job kept reporting `RUNNING` for ~25 s after the TaskManager was killed**, until the
  cluster noticed. The slot's `active=false` flipped within one sample. For alerting, the slot is
  the more reliable signal than the job state.
- **Duplicates only show up if you look for them.** ClickHouse collapses duplicates twice on its
  own: inside one insert batch (`optimize_on_insert`, default on) and in background merges. The demo
  script pauses merges and turns `optimize_on_insert` off for the sink user, counts
  `count() - uniqExact(resource_id, _version)`, then restores both. In normal operation you would not
  see these rows for long. (My first attempts counted rows without doing this and got misleading
  numbers, including a negative duplicate count.)
- **`verify.sh` compares final state, not history.** It would not notice a lost intermediate change
  that a later change superseded. That is acceptable here because only the latest version per key
  matters, but it is worth saying out loud.
- **Untested:** a Postgres outage longer than the restart budget (3 attempts x 5 s) would leave the
  job `FAILED`. I only tested a container restart that recovered within one attempt.
- **This is a restart, not a failover.** `docker compose restart postgres` brings back the *same*
  container, same disk, same data directory — the replication slot was never destroyed. A real
  failover (a different node, previously a standby, promoted to primary) is a materially different
  and more dangerous case: a promoted node typically has no usable slot at all, and Postgres before
  17 has no built-in way to sync one to it, so committed changes in the gap can be lost silently, not
  just delayed. Not reproduced here — see the version matrix note in the main README.

## Where exactly-once ends
- **Flink source and state:** exactly-once. The LSN in the checkpoint and the slot's confirmed position move together.
- **Sink into ClickHouse:** at-least-once. After a restore, changes after the checkpoint are sent again (the 40 and 80 above).
- **Correctness:** recovered by `ReplacingMergeTree`. A replayed change has the same `resource_id` and `_version` (its LSN), so it collapses to one row.

## Schema changes mid-stream (limitation, not implemented)
If a column is added in Postgres, the running job does not pick it up. The typed parser reads a fixed
set of fields, so the new column is ignored, and the ClickHouse table would need the column added
by hand plus a job redeploy. A dropped or renamed column the parser reads would break it.
