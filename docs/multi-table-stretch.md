# Stretch exercise: scaling toward many tables

The main pipeline (Phases 0-6) is one table, one slot, hand-written Java per table. This
exercise prototypes the pattern for scaling to many tables, without touching that pipeline:
a second, independent job (`MultiTableCdcJob`) on its own branch, reading two more tables of
genuinely different shapes through ONE shared replication slot, with generic (config-driven,
not hand-typed) parsing and ClickHouse mapping. See the design discussion in the conversation
this came from for the full "scaling to 200 tables" answer this prototypes one slice of.

## What's new

- **Two more Postgres tables**, deliberately different from `resource_inventory`:
  `network_interfaces` (booleans, a nullable IP) and `snapshot_jobs` (a nullable integer, two
  independent timestamps). `postgres/init/02-multi-table-stretch.sql`.
- **One publication, one slot, two tables**: `flink_pub_multi` / `flink_multi_slot`, entirely
  separate from `flink_pub` / `flink_slot` used by the Phase 1-6 job — proven by running both
  jobs at once and confirming both slots stay independently `active=true`.
- **`scripts/generate_clickhouse_ddl.py`**: introspects a Postgres table via
  `information_schema` and emits its ClickHouse `ReplacingMergeTree` mirror, current-state
  view, and grant — instead of hand-writing `01-schema.sql` per table. `--mapping-out` emits
  the *same* introspection as the Java job's config (`table-mappings.json`), so the DDL and
  the mapping come from one source, not two hand-kept copies.
- **`GenericChange` / `GenericDebeziumJsonParser`**: the table-agnostic counterpart of
  `ResourceChange` / `DebeziumJsonParser`. One parser class walks whatever fields a row
  actually has (via Jackson's `JsonNode.fields()`), instead of a fixed field list per table.
- **`TableMapping` / `GenericClickHouseMapper`**: the table-agnostic counterpart of
  `ResourceChangeMapper`. One mapper class, built from a `TableMapping` loaded from JSON at
  startup — adding a table means adding a JSON entry, not a Java class.
- **`MultiTableCdcJob`**: one source reading both tables (`tableList` built from the config),
  filtered and parsed generically, then split by table name into Flink side outputs, each
  feeding its own `ClickHouseAsyncSink` built from that table's mapping.

## Verified

Fresh stack, both jobs (`CdcJob` and `MultiTableCdcJob`) submitted and left running:

| | before | after 30 inserts/table + resource_inventory touches |
|---|---|---|
| `resource_inventory_current` | 200 | 200 (untouched by the multi-table job) |
| `network_interfaces_current` | 60 | 90 |
| `snapshot_jobs_current` | 40 | 70 |

Postgres and ClickHouse matched exactly at every count, `verify.sh` passed, and both
`flink_slot` and `flink_multi_slot` stayed `active=true` throughout with distinct PIDs —
two jobs, two slots, no interference.

Insert/update/delete lifecycle tested directly on both new tables: booleans convert correctly
(`is_primary: true` -> ClickHouse `UInt8` 1), nulls pass through (`public_ip`, `size_gb`,
`completed_at`), and a delete's tombstone row retains the last known values, not nulls —
the same behavior `DebeziumJsonParser` gives the hand-typed pipeline.

5 unit tests (`GenericDebeziumJsonParserTest`) use real captured envelopes from both table
shapes, proving the same parser class handles both without table-specific code — mirroring
Phase 6's fixture-test approach for the hand-typed parser.

## What broke, and the actual fix

First run failed immediately: `ClassCastException: Long cannot be cast to Integer` in the
ClickHouse client's row writer. The generic parser only knows "this JSON value is an integral
number," so it always produces a `Long`; the client is strict about matching Java's boxed
type to the column's declared width (`Int32` wants `Integer`, not `Long`). Fixed in
`GenericClickHouseMapper.coerce()`: narrow to the right boxed type based on the *declared*
ClickHouse type from the mapping config, not by guessing from the value. This is the one
piece of type-specific logic the generic path still needs — everything else (column names,
nullability, which columns exist at all) comes from config.

## What this does and doesn't prove, honestly

**Proves:** many tables can share one slot with no per-table Java code, using config
generated from the same schema introspection that generates the ClickHouse DDL, and that two
independent slot-groups (two jobs) don't interfere with each other.

**Doesn't prove, and would need more work for a real 200-table system:**
- **Job-per-group at scale.** This is two jobs; running 10-15 (one per slot group, per the
  grouping-by-blast-radius argument) is the same pattern repeated, not something new to build,
  but it's untested here.
- **Onboarding a table into a *running* group.** Both tables here were in the publication
  and the job's config from a cold start. Adding a third table to `flink_pub_multi` without
  restarting `MultiTableCdcJob` — and whether Flink CDC's incremental source supports that
  cleanly — is untested.
- **Type coverage.** `PG_TO_CH` in the DDL generator handles the handful of types this
  project uses. Arrays, JSON columns, enums, and numeric precision edge cases are unhandled
  and would need real work, not just more `if` branches.
- **The Debezium `schema` block is unused.** The generic parser infers types from JSON alone
  (`includeSchema=false`, to keep messages small). This works because ClickHouse's declared
  column type is the actual source of truth for width/precision — but it does mean a Postgres
  `smallint` and a `bigint` are indistinguishable to the parser until the mapper narrows them;
  get the mapping config wrong and you won't find out until a cast exception at runtime, not
  at parse time.
