# flink-cdc-postgres-clickhouse

Change Data Capture from PostgreSQL into ClickHouse using Apache Flink CDC. An operational cloud resource inventory in Postgres is replicated into ClickHouse in near real time, so cost rollups by team, account and region run on the analytical store instead of the OLTP database.

```
Postgres (logical replication) ──> Flink CDC job (DataStream API) ──> ClickHouse (ReplacingMergeTree)
```

## Status

In progress: Phases 0 (infrastructure), 1 (CDC source), 2 (typed change events), 3 (ClickHouse sink) and 4 (checkpointing and failure demo) are done. The full phase-by-phase implementation plan is in
[flink-cdc-postgres-clickhouse-plan.md](flink-cdc-postgres-clickhouse-plan.md). The implementation is built one phase at a time.

## Stack

- PostgreSQL 16 (`wal_level=logical`, `REPLICA IDENTITY FULL`)
- Apache Flink 2.2.1 and Flink CDC 3.6.0 (Java 21, Maven, DataStream API)
- ClickHouse 26.8 (LTS)
- Docker Compose, everything runs locally

## Planned layout

```
docker-compose.yml
postgres/init/      schema, seed data, publication
clickhouse/init/    ReplacingMergeTree target table
flink-job/          Maven project with the CDC job
scripts/            simulator, verify, failure demo, slot health
docs/
```

## Version compatibility

Flink, Flink CDC, `flink-connector-jdbc` and the ClickHouse JDBC driver versions must line up.

| Component | Version | Status |
|---|---|---|
| Java | 21 (`eclipse-temurin` 21.0.12 in `flink:2.2.1-java21`) | verified |
| Flink | 2.2.1 | verified |
| Flink CDC (`flink-connector-postgres-cdc`) | 3.6.0-2.2 | verified (Phases 1-2) |
| `flink-shaded-guava` (bundled) | 31.1-jre-17.0 | required, see note |
| Debezium (transitive) | 1.9.8.Final | verified |
| PostgreSQL | 16.15 | verified |
| ClickHouse server | 26.8.9 | verified |
| ClickHouse Flink connector (`flink-connector-clickhouse-2.0.0`, `all` classifier) | 0.2.0 | verified (Phase 3); DataStream, at-least-once |

Note: Flink CDC 3.6.0 is compiled against shaded guava 31, which Flink 1.20 shipped at runtime but
Flink 2.x does not. Without bundling it the source fails with `NoClassDefFoundError:
org/apache/flink/shaded/guava31/...ThreadFactoryBuilder`. It is declared directly in `pom.xml`
so it wins Maven's version mediation. Flink CDC 3.6.0 supports Flink up to 2.2.x (not 2.3).

## Quick start

```bash
docker compose up -d
docker compose ps        # postgres, clickhouse, jobmanager healthy
```

Sanity commands:

```bash
# Postgres: logical replication on, 200 seed rows
docker exec cdc-postgres psql -U postgres -d inventory -c "SHOW wal_level;"
docker exec cdc-postgres psql -U postgres -d inventory -c "SELECT count(*) FROM resource_inventory;"

# CDC role can read the published tables
docker exec -e PGPASSWORD=cdc_pass cdc-postgres psql -h localhost -U cdc_user -d inventory -c "SELECT count(*) FROM cdc_heartbeat;"

# ClickHouse over HTTP
curl "localhost:8123/?query=SELECT%201&user=default&password=clickhouse"

# Flink UI: http://localhost:8081 (1 TaskManager, 4 slots)
curl -s localhost:8081/overview

# Replication slot health (add --watch to refresh every 2s)
scripts/slot_health.sh
```

Build and run the CDC job (Maven runs in Docker, no local Java needed):

```bash
scripts/build.sh
docker exec cdc-jobmanager flink run -d /opt/flink/usrlib/cdc-job.jar
docker logs -f cdc-taskmanager      # typed ResourceChange events + CDC lag lines
```

Query ClickHouse (raw append-only table vs. current state):

```bash
ch() { curl -s "localhost:8123/?user=default&password=clickhouse" --data-binary "$1"; }
ch "SELECT count() FROM cdc.resource_inventory_current"                 # current state, deletes removed
ch "SELECT * FROM cdc.resource_inventory WHERE resource_id='res-0010'"  # every version of a row
ch "OPTIMIZE TABLE cdc.resource_inventory FINAL"                        # force merges
```

Kill-and-recover demo (simulator running, TaskManager killed, `verify.sh` must match):

```bash
scripts/fresh_start.sh && scripts/failure_demo.sh taskmanager    # or: postgres
```

See [docs/failure-demo.md](docs/failure-demo.md) for results, and [docs/heartbeat-findings.md](docs/heartbeat-findings.md) for the heartbeat experiment results.

## Note

Credentials in this repo (for example `cdc_user` / `cdc_pass`) are local demo placeholders only.
