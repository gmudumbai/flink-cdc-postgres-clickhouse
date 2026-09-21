# Implementation Plan: `flink-cdc-postgres-clickhouse`

**Change Data Capture from PostgreSQL into ClickHouse using Apache Flink CDC — an operational resource inventory replicated into an analytical store, in near real time.**

## Instructions for the implementing model

You are implementing this project **one phase at a time**. After completing a phase:

1. Run the acceptance check for that phase and paste the actual output.
2. Summarize what you built in 3–5 sentences, in plain language.
3. **Stop and wait.** The user will ask questions about the phase before you continue. Do not start the next phase until told to.

Conventions:
- Everything runs locally via Docker Compose. No cloud resources, no managed databases.
- Java 21, Maven, Apache Flink 2.2.x, Flink CDC 3.6.0-2.2 (`org.apache.flink:flink-connector-postgres-cdc`), and the official ClickHouse Flink connector (`com.clickhouse.flink:flink-connector-clickhouse-2.0.0`, DataStream, at-least-once). *Deviation from the original plan (Java 17, Flink 1.20, flink-connector-jdbc): upgraded after Phase 2; see README version table.*
- **Version compatibility is the single biggest risk in this project.** Before writing `pom.xml`, verify on Maven Central which Flink CDC 3.x release targets the chosen Flink 1.20 patch, and which `flink-connector-jdbc` version matches. If a combination fails at runtime with `NoSuchMethodError` or similar, that is a version mismatch — fix the matrix rather than working around it. Record the working combination in the README.
- Stock images only: `postgres:16`, `clickhouse/clickhouse-server:26.8`, `flink:2.2.1-java21`.
- Use the **DataStream API** for the CDC source and the sink. Flink SQL would be shorter; the README will explain why DataStream was chosen (see Phase 6).
- Keep it to one Flink job and one primary table. Small and readable beats complete.
- Commit at the end of every phase: `phase 2: typed change events with before/after images`.
- If this plan conflicts with what actually works in the current versions, do what works and note the deviation in your phase summary.

### Domain

Postgres holds an operational **cloud resource inventory** — the kind of table a platform team writes to constantly:

```sql
CREATE TABLE resource_inventory (
  resource_id   TEXT PRIMARY KEY,
  account_id    TEXT NOT NULL,
  region        TEXT NOT NULL,
  resource_type TEXT NOT NULL,     -- 'ec2', 'rds', 'ebs'
  instance_size TEXT,
  team          TEXT,
  state         TEXT NOT NULL,     -- 'running', 'stopped', 'terminated'
  hourly_cost   NUMERIC(10,4),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

ClickHouse becomes the analytical mirror: same rows, queryable for cost rollups by team, account, and region without touching the OLTP database. This is the real reason CDC exists — the transactional store is the source of truth, the analytical store answers questions the transactional store would choke on.

Repo layout target:

```
flink-cdc-postgres-clickhouse/
├── README.md
├── docker-compose.yml
├── postgres/
│   ├── postgresql.conf          # wal_level=logical
│   └── init/01-schema.sql       # table, REPLICA IDENTITY, seed rows, publication
├── clickhouse/
│   └── init/01-schema.sql       # ReplacingMergeTree target
├── scripts/
│   ├── simulate_changes.py      # continuous inserts/updates/deletes
│   ├── verify.sh                # side-by-side row comparison
│   ├── slot_health.sh           # replication slot lag / activity check
│   └── churn_other_table.py     # writes to an UNmonitored table (heartbeat demo)
├── flink-job/
│   ├── pom.xml
│   └── src/main/java/com/example/cdc/
│       ├── CdcJob.java
│       ├── model/ResourceChange.java
│       ├── DebeziumJsonParser.java
│       └── ClickHouseSinkFactory.java
│   └── src/test/java/com/example/cdc/
└── docs/
```

Time budget: ~7 hours.

---

## Phase 0 — Infrastructure: Postgres with logical replication, ClickHouse, Flink (≈50 min)

### Goal
All three systems up, Postgres configured for logical decoding, seed data in place, ClickHouse reachable. No Flink job yet.

### Build
1. `docker-compose.yml`:
   - **postgres**: `postgres:16`, port 5432. Override config so that `wal_level=logical`, `max_replication_slots=4`, `max_wal_senders=4`. Do this either with `command: ["postgres", "-c", "wal_level=logical", ...]` or a mounted `postgresql.conf` — the command flags are simpler and less brittle.
   - **clickhouse**: expose 8123 (HTTP) and 9000 (native). Set a default user/password via env. Mount `clickhouse/init/` into `/docker-entrypoint-initdb.d/`.
   - **jobmanager** / **taskmanager**: `flink:1.20-java17`, 4 task slots, UI on 8081, a shared volume for checkpoints, and `./flink-job/target` mounted so the built jar can be submitted.
2. `postgres/init/01-schema.sql`:
   - Create the `resource_inventory` table above.
   - `ALTER TABLE resource_inventory REPLICA IDENTITY FULL;` — so UPDATE and DELETE events carry the full previous row, not just the primary key. This is a deliberate choice with a real WAL cost; see the learning checkpoint.
   - A **heartbeat table**, needed in Phase 1:
     ```sql
     CREATE TABLE cdc_heartbeat (
       id INT PRIMARY KEY,
       ts TIMESTAMPTZ NOT NULL
     );
     INSERT INTO cdc_heartbeat VALUES (1, now());
     ```
   - A **noise table** that is deliberately *not* published, used to demonstrate the quiet-table WAL problem:
     ```sql
     CREATE TABLE unrelated_writes (
       id BIGSERIAL PRIMARY KEY,
       payload TEXT NOT NULL,
       created_at TIMESTAMPTZ NOT NULL DEFAULT now()
     );
     ```
   - `CREATE PUBLICATION flink_pub FOR TABLE resource_inventory, cdc_heartbeat;` — the heartbeat table must be in the publication or its writes won't advance the slot.
   - A dedicated CDC role rather than reusing the superuser:
     ```sql
     CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'cdc_pass';
     GRANT SELECT ON resource_inventory, cdc_heartbeat TO cdc_user;
     GRANT UPDATE ON cdc_heartbeat TO cdc_user;   -- heartbeat action query needs to write
     ```
     Note in a comment: the publication is pre-created here precisely so `cdc_user` does not need `CREATE` on the database. This is the production pattern.
   - Seed ~200 rows across 3 accounts, 3 teams, 2 regions, mixed resource types and states.
3. `clickhouse/init/01-schema.sql`: leave it a stub for now (a `CREATE DATABASE cdc;` is enough) — the real target table lands in Phase 3.
4. `scripts/slot_health.sh` — the single most important operational query for this pipeline. It should run:
   ```sql
   SELECT
     slot_name,
     active,
     active_pid,
     pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))      AS retained_wal,
     pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS unconfirmed,
     restart_lsn,
     confirmed_flush_lsn
   FROM pg_replication_slots;
   ```
   Print it in a readable form. Accept an optional `--watch` flag that re-runs every 2 seconds. Handle the case where no slot exists yet (before the job's first run) without erroring.
5. Sanity commands in the README.

### Acceptance check
- `docker compose up -d`, all three healthy.
- `SHOW wal_level;` in Postgres returns `logical`.
- `SELECT count(*) FROM resource_inventory;` returns 200.
- `SELECT 1` against ClickHouse over HTTP (`curl localhost:8123/?query=SELECT%201`) returns `1`.
- Flink UI at `localhost:8081` shows 1 TaskManager, 4 slots.
- `psql -U cdc_user` connects and can read both published tables.
- `scripts/slot_health.sh` runs and reports "no replication slots" cleanly.

### Learning checkpoint (user asks about)
- What the write-ahead log is, and why `wal_level=logical` is needed on top of the WAL Postgres already writes for crash recovery.
- What a **publication** is and what a **replication slot** is — and specifically what a slot guarantees (Postgres will retain WAL until the consumer confirms it has read past it).
- `REPLICA IDENTITY FULL`: what it adds to every UPDATE and DELETE record, and what it costs. Ask the model to reason about WAL volume on a wide table under heavy update load. Then ask the sharper question: does *this* sink actually need before-images, given that the ClickHouse table is ordered by `resource_id` and a delete only needs key + version + tombstone? Keep it on for this project because it makes change events legible and supports future sinks that do need before-images — but be able to defend it as a choice, not an accident.
- Why `pgoutput` and not `decoderbufs` or `wal2json` — and why that choice is what makes this work unchanged on RDS, Aurora, and Cloud SQL.
- Why the publication and the CDC role are pre-created in SQL rather than letting the connector create them (least privilege).
- The three columns in `slot_health.sh` and what each one means: `active` (is a consumer attached), `retained_wal` (how much WAL Postgres cannot recycle), `unconfirmed` (how far behind the consumer is). Which one pages you at 3am.

---

## Phase 1 — The CDC source: snapshot then stream (≈60 min)

### Goal
A Flink job that connects to Postgres, performs an initial snapshot of the table, then streams ongoing changes — printing raw Debezium JSON.

### Build
1. Maven project with `flink-streaming-java`, `flink-clients`, `flink-connector-postgres-cdc`, Jackson. Shade plugin for a fat jar. Watch for relocation conflicts — Flink CDC bundles Debezium and Kafka Connect classes; if shading fights back, use the pre-shaded CDC artifact rather than fighting the plugin.
2. `CdcJob.java`: build a Postgres CDC source using the **incremental snapshot** builder (`PostgresSourceBuilder.PostgresIncrementalSource`, the newer parallel source — not the legacy `PostgreSQLSource`). Configure:
   - hostname, port, database, schema `public`, table `public.resource_inventory`, user, password
   - `slotName("flink_slot")`
   - `decodingPluginName("pgoutput")` — this is what allows the stock `postgres:16` image to work with no extra plugin installed
   - `startupOptions(StartupOptions.initial())`
   - `deserializer(new JsonDebeziumDeserializationSchema())`
   - **Heartbeat**, passed through as Debezium properties:
     ```java
     Properties debezium = new Properties();
     debezium.setProperty("heartbeat.interval.ms", "5000");
     debezium.setProperty("heartbeat.action.query",
         "UPDATE cdc_heartbeat SET ts = now() WHERE id = 1");
     // ...builder.debeziumProperties(debezium)
     ```
     Without this, a quiet `resource_inventory` table means the slot never confirms progress even while the rest of the database writes heavily — WAL accumulates until the disk fills. The action query forces a published write so the slot can advance. Use 5 seconds here so the effect is visible in a demo; production values are typically 10–60 seconds.
3. Filter heartbeat records out of the business stream — they arrive as ordinary change events on `cdc_heartbeat` and must not reach the sink. Filter on the `source.table` field in the Debezium envelope, not on a guessed shape of the payload.
4. `env.fromSource(source, WatermarkStrategy.noWatermarks(), "postgres-cdc")` → heartbeat filter → `.print()`.
5. Note in code comments: parallelism > 1 is only meaningful during the snapshot phase; the streaming phase is single-reader because there is one replication slot.
6. `scripts/churn_other_table.py`: writes a few hundred rows per second to `unrelated_writes` (the unpublished table). Used only for the heartbeat demo below.

### Acceptance check
- Submit the job. TaskManager logs show 200 snapshot records with `"op":"r"` (read — meaning "this came from the snapshot, not the log").
- With the job still running, `INSERT` a row in Postgres via psql → an `"op":"c"` record appears within a second or two.
- `UPDATE` a row → `"op":"u"` with both `before` and `after` populated (this is `REPLICA IDENTITY FULL` paying off).
- `DELETE` a row → `"op":"d"` with `before` populated and `after` null.
- `scripts/slot_health.sh` shows `flink_slot` with `active = t` and a small, stable `unconfirmed` value.
- **Heartbeat proof (do this one carefully, it is the best story in the phase):**
  1. Set `heartbeat.interval.ms` to `0` (disabled), rebuild, run the job, and leave `resource_inventory` completely untouched.
  2. Run `churn_other_table.py` for two minutes.
  3. `slot_health.sh --watch` → `retained_wal` climbs steadily even though the monitored table has had zero changes and the consumer is healthy.
  4. Re-enable the heartbeat at 5000 ms, restart the job, repeat.
  5. `retained_wal` now stays flat. Capture both outputs for `docs/`.

### Learning checkpoint
- The two phases of CDC: consistent snapshot, then log tailing — and the hard part, which is handing off between them without gaps or duplicates. Ask the model to explain how the incremental snapshot algorithm avoids locking the table.
- What `op` values mean (`r`, `c`, `u`, `d`) and what the `source` block in the envelope carries (LSN, transaction id, commit timestamp).
- What an LSN is and why it is the right notion of "version" for a Postgres row.
- Why the streaming phase can't be parallelized across many readers.
- What happens if the Flink job is down for an hour and then restarts — where does it resume from, and what is the cost of that guarantee?
- **The heartbeat**, in the user's own words: why does a *healthy, connected* consumer still fail to advance the slot when the monitored table is quiet? The answer is the core of it — the slot advances only when the consumer confirms an LSN, the consumer only sees records for published tables, and so a busy database with a quiet published table produces WAL that nothing will ever confirm past. Ask what the action query is doing and why it must target a table inside the publication.
- Why the heartbeat interval is a tradeoff rather than "set it low" (each heartbeat is a real write and a real WAL record).
- What monitoring you would put on this in production, and what the alert threshold would be on `retained_wal`.

---

## Phase 2 — Typed change events (≈50 min)

### Goal
Turn raw Debezium JSON into a clean Java model the sink can consume, carrying everything ClickHouse will need to resolve row versions.

### Build
1. `model/ResourceChange.java` — a proper Flink POJO (no-arg constructor, public fields or getters/setters):
   - business columns: `resourceId`, `accountId`, `region`, `resourceType`, `instanceSize`, `team`, `hourlyCost`, `updatedAt`
   - CDC metadata: `op` (char), `lsn` (long), `sourceTsMs` (long — when the change committed in Postgres), `ingestTsMs` (long — when Flink processed it)
   - `isDeleted` (byte, 0 or 1)
2. `DebeziumJsonParser.java` — a `MapFunction<String, ResourceChange>`:
   - For `c`, `u`, `r`: read the `after` object.
   - For `d`: read the `before` object and set `isDeleted = 1`.
   - Pull `lsn` and `ts_ms` from the `source` block. For snapshot records the LSN may be null or the snapshot-start LSN — handle that explicitly rather than letting it NPE, and decide what version a snapshot row should get (hint: it must sort *below* any subsequent real change).
   - Set `ingestTsMs` from `System.currentTimeMillis()`.
3. Add a `.map()` that logs end-to-end lag (`ingestTsMs - sourceTsMs`) every N records so the user can watch latency.
4. Keep `.print()` as the sink for now.

### Acceptance check
- Output lines show typed fields, correct `op`, and a plausible lag (single-digit or low-double-digit milliseconds locally).
- A delete produces a record with the original column values populated and `isDeleted=1`, not a row of nulls.

### Learning checkpoint
- Why a delete has to be turned into a *row* rather than an absence — the downstream store has no way to "receive nothing."
- Why `lsn` is a better version column than `updated_at` (clock skew, same-millisecond updates, application code that forgets to set it).
- The snapshot-versus-stream ordering problem: if a row is snapshotted and then immediately updated, what guarantees the update wins downstream?
- Why the POJO shape matters to Flink (serializer selection).

---

## Phase 3 — The ClickHouse sink (≈100 min)

### Goal
Land changes in ClickHouse using `ReplacingMergeTree`, so that an append-only stream of inserts produces a correct current-state view including deletes.

### Build
1. `clickhouse/init/01-schema.sql`:
   ```sql
   CREATE DATABASE IF NOT EXISTS cdc;

   CREATE TABLE cdc.resource_inventory (
     resource_id    String,
     account_id     String,
     region         String,
     resource_type  String,
     instance_size  String,
     team           String,
     state          String,
     hourly_cost    Decimal(10,4),
     updated_at     DateTime64(3),
     _version       UInt64,
     _is_deleted    UInt8
   ) ENGINE = ReplacingMergeTree(_version, _is_deleted)
   ORDER BY resource_id;
   ```
   Verify the two-argument `ReplacingMergeTree(version, is_deleted)` form is supported by the ClickHouse version pinned in Compose; it requires a reasonably recent release. If not, pin a newer image.

   Also create a convenience view:
   ```sql
   CREATE VIEW cdc.resource_inventory_current AS
   SELECT * FROM cdc.resource_inventory FINAL WHERE _is_deleted = 0;
   ```
2. `ClickHouseSinkFactory.java`: *(implemented with the official ClickHouse Flink connector instead of `flink-connector-jdbc`; `team`, `instance_size` and `hourly_cost` are `Nullable` to mirror Postgres; batching is `maxBatchSize`/`maxTimeInBufferMS`, there is no retry count. The JDBC description below is the original plan.)* a JDBC sink via `flink-connector-jdbc`:
   - URL `jdbc:clickhouse://clickhouse:8123/cdc`, driver `com.clickhouse.jdbc.ClickHouseDriver`
   - A plain `INSERT INTO cdc.resource_inventory (...) VALUES (?, ?, ...)` — **always an insert, never an update or delete**
   - `JdbcExecutionOptions`: batch size 500, batch interval 2000 ms, max retries 3
   - Map `_version` from `lsn`, `_is_deleted` from `isDeleted`
3. Replace `.print()` with this sink. Keep a parallel `.print()` on a small sample if it helps debugging, then remove it.

### Acceptance check
- Restart everything clean (drop the replication slot first, or use a fresh volume). Run the job.
- `SELECT count() FROM cdc.resource_inventory_current` → 200, matching Postgres.
- `UPDATE resource_inventory SET state='stopped', hourly_cost=0 WHERE resource_id='...'` → within a few seconds, `SELECT * FROM cdc.resource_inventory WHERE resource_id='...'` shows **two** rows (raw table), while the `_current` view shows **one**, with the new values.
- `DELETE` a row → raw table gains a row with `_is_deleted=1`; the `_current` view count drops by one.
- `OPTIMIZE TABLE cdc.resource_inventory FINAL;` then re-query the raw table — the superseded rows are gone.

### Learning checkpoint
This is the richest phase. Worth a long question session.
- Why ClickHouse has no real `UPDATE` or `DELETE` in the OLTP sense, and what `ALTER TABLE ... UPDATE` actually does (rewrites parts — expensive, asynchronous, not a row operation).
- How `ReplacingMergeTree` works: dedupe by `ORDER BY` key, keep the highest `_version`, drop rows with `_is_deleted=1` — **but only when parts merge**, which happens on ClickHouse's schedule, not yours.
- Therefore: what `FINAL` costs at query time, and why production systems often prefer an `AggregatingMergeTree`, a materialized view, or accepting eventual convergence over `FINAL` on every query.
- Why batching matters enormously: each insert creates a part; thousands of tiny inserts produce a "too many parts" error. Ask what batch size and interval trade against each other (latency vs. part count).
- The key insight for the interview: **an at-least-once sink plus a version-based dedupe engine gives you effectively-once results.** Flink can replay the same change after a failure; ClickHouse will collapse the duplicate because the `_version` is identical. This is why the design tolerates a non-transactional sink.

---

## Phase 4 — Checkpointing, failure, and a correctness demo (≈60 min)

### Goal
Prove the pipeline survives a crash without losing or corrupting data, and produce a repeatable demo for the README.

### Build
1. Enable checkpointing: `env.enableCheckpointing(10_000)` in exactly-once mode, checkpoint storage on the shared Docker volume, min-pause a few seconds, fixed-delay restart strategy (3 attempts, 5s).
2. `scripts/simulate_changes.py`: continuously applies a mix of inserts, updates and deletes against Postgres at a configurable rate, and writes an append-only local log of every operation it performed.
3. `scripts/verify.sh`: runs the same aggregate on both sides and diffs them — e.g. per-team row count and `sum(hourly_cost)` from Postgres, and from `cdc.resource_inventory_current`. Exit non-zero on mismatch.
4. `docs/failure-demo.md` with exact steps:
   - Start the simulator at ~20 changes/sec.
   - `docker compose kill taskmanager`, wait 20 seconds, `docker compose up -d taskmanager`.
   - Confirm from the logs which checkpoint the job restored from.
   - Run `slot_health.sh --watch` throughout. While the TaskManager is down, `active` flips to `f` and `retained_wal` grows; after restart it reconnects and drains. Capture this — it is the clearest possible illustration of what a slot is for.
   - Confirm the replication slot's `restart_lsn` did not jump past unprocessed data.
   - Stop the simulator, wait for drain, run `verify.sh` → tables match.
   - Note how many duplicate rows landed in the raw table as a result of replay, and show that the `_current` view is still correct.
5. Also demo the kill-the-*database*-connection case: `docker compose restart postgres` and confirm the job reconnects and resumes from the slot.

### Acceptance check
- `verify.sh` exits 0 after the kill-and-recover cycle, with the simulator having run throughout.
- Paste the before/after row counts and the restored checkpoint ID.

### Learning checkpoint
- What Flink stores in a CDC checkpoint (the LSN / offset, plus snapshot progress if mid-snapshot) and how that coordinates with the Postgres replication slot.
- Why the slot's `restart_lsn` only advances after a successful checkpoint, and what would break if it advanced eagerly.
- Where exactly-once ends: Flink's source and state are exactly-once, the JDBC insert into ClickHouse is at-least-once, and correctness is recovered by the dedupe engine. Ask the model to state precisely which component provides which guarantee — this is exactly how the question gets asked in an interview.
- What happens if the *schema* changes in Postgres mid-stream (add a column). Don't implement it; understand it and write it up as a limitation.

---

## Phase 5 (stretch) — A live analytical view (≈40 min)

Only if the earlier phases went fast. This is what makes the repo feel like it has a point rather than being a plumbing demo.

### Build
1. A ClickHouse materialized view maintaining per-team, per-region current hourly spend, or a simple `SELECT team, count(), sum(hourly_cost) FROM cdc.resource_inventory_current WHERE state='running' GROUP BY team`.
2. A 20-line HTML page or a `watch`-based shell loop that re-runs the query every two seconds, so the demo GIF shows numbers moving as the simulator mutates Postgres.

### Learning checkpoint
- Why the aggregate cannot simply be maintained by a materialized view over the raw CDC table (an MV fires on insert and does not see the replacement semantics), and what the correct patterns are.

---

## Phase 6 — Tests, CI, README (≈50 min)

### Build
1. A JUnit test for `DebeziumJsonParser`: feed three fixture JSON strings (a `c`, a `u` with before/after, a `d`) captured from an actual run, assert the parsed `ResourceChange` fields including `isDeleted` and `lsn`. Fixtures go in `src/test/resources/`.
2. Optionally one end-to-end test using Testcontainers (Postgres + ClickHouse). Only attempt this if time genuinely remains; it is the most likely thing to eat an hour.
3. `.github/workflows/ci.yml`: Java 17, `mvn -B package`.
4. `README.md`, in this order:
   - One-sentence pitch.
   - **The problem:** the operational inventory lives in Postgres; analytical questions about cost need columnar scans across millions of rows; running those on the OLTP database is how you take production down. CDC keeps a ClickHouse mirror seconds behind without dual writes. Two short paragraphs.
   - Architecture diagram (Mermaid): Postgres WAL → replication slot → Flink CDC source → parse → JDBC batch sink → ClickHouse ReplacingMergeTree → current-state view.
   - **Run it:** three commands, plus the simulator.
   - **What you'll see:** sample output, the two-rows-in-raw / one-row-in-view demo, and a GIF of the live aggregate.
   - **Design decisions**, each one or two sentences: `pgoutput` over `decoderbufs` (stock image, and the only option on RDS/Aurora/Cloud SQL); `REPLICA IDENTITY FULL` and its WAL cost, kept deliberately; pre-created publication and least-privilege `cdc_user`; heartbeat interval and action query; incremental snapshot over lock-based; LSN as version column; append-only sink into `ReplacingMergeTree` rather than attempting updates; batch size and interval; `FINAL` in a view versus query-time cost; DataStream over Flink SQL (chosen for explicit control over parsing and metadata, and because the SQL version hides exactly the mechanics this project exists to demonstrate — say so plainly).
   - **Operating this**, a short standalone section — this is the part most CDC demos omit and the part that signals production experience:
     - The `pg_replication_slots` query, verbatim, with what each column means.
     - What to alert on: slot inactive for more than N minutes; `retained_wal` above a threshold sized against the WAL volume available.
     - The quiet-table failure mode and the heartbeat that fixes it, with the two captured `slot_health.sh` outputs from Phase 1 side by side.
     - The cleanup rule: dropping the Flink job does **not** drop the slot. `SELECT pg_drop_replication_slot('flink_slot');` — and the consequence of forgetting.
     - A one-paragraph translation to managed Postgres: `rds.logical_replication=1` plus `rds_replication` grant on RDS/Aurora, `cloudsql.logical_decoding` on Cloud SQL, both requiring a reboot.
   - **Guarantees:** one short table — source exactly-once, sink at-least-once, end state effectively-once via version dedupe.
   - **Failure demo** (link to `docs/failure-demo.md`).
   - **Not here / next steps:** schema evolution, multiple tables and a shared slot, ClickHouse cluster and replication, monitoring replication lag and slot size, backfill/reseed procedure, `TOAST`ed column handling. Be specific; vague limitations read as hedging, specific ones read as experience.
   - **Version matrix that actually worked** — Flink, Flink CDC, JDBC connector, ClickHouse driver, ClickHouse server. Future readers will thank you and it signals you hit the problem and solved it.
5. GitHub topics: `apache-flink`, `flink-cdc`, `change-data-capture`, `clickhouse`, `postgresql`, `streaming`.

### Learning checkpoint (interview-framing pass)
The user will ask the model to produce, for each design decision, the one-sentence answer to give an interviewer plus the tradeoff accepted — and to draft answers to the five questions this project most invites:
1. Walk me through what happens between `COMMIT` in Postgres and the row being queryable in ClickHouse.
2. How do you avoid losing data if the Flink job dies for an hour?
3. Your sink isn't transactional. How is the result still correct?
4. Why not just dual-write from the application?
5. What breaks first when you scale this from one table to two hundred?
6. Your CDC consumer is healthy and connected, the monitored table has had no writes all day, and the database disk is filling up. What's happening?
