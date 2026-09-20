# flink-cdc-postgres-clickhouse

Change Data Capture from PostgreSQL into ClickHouse using Apache Flink CDC. An operational cloud resource inventory in Postgres is replicated into ClickHouse in near real time, so cost rollups by team, account and region run on the analytical store instead of the OLTP database.

```
Postgres (logical replication) ──> Flink CDC job (DataStream API) ──> ClickHouse (ReplacingMergeTree)
```

## Status

In progress: Phases 0 (infrastructure), 1 (CDC source) and 2 (typed change events) are done. The full phase-by-phase implementation plan is in
[flink-cdc-postgres-clickhouse-plan.md](flink-cdc-postgres-clickhouse-plan.md). The implementation is built one phase at a time.

## Stack

- PostgreSQL 16 (`wal_level=logical`, `REPLICA IDENTITY FULL`)
- Apache Flink 1.20.x and Flink CDC 3.x (Java 17, Maven, DataStream API)
- ClickHouse (JDBC sink)
- Docker Compose, everything runs locally

## Planned layout

```
docker-compose.yml
postgres/init/      schema, seed data, publication
clickhouse/init/    ReplacingMergeTree target table
flink-job/          Maven project with the CDC job
scripts/            change simulator, verify, replication slot health
docs/
```

## Version compatibility

Flink, Flink CDC, `flink-connector-jdbc` and the ClickHouse JDBC driver versions must line up.

| Component | Version | Status |
|---|---|---|
| Flink | 1.20.5 | verified |
| Flink CDC (`flink-connector-postgres-cdc`) | 3.6.0-1.20 | verified (Phase 1) |
| Debezium (transitive) | 1.9.8.Final | verified |
| `flink-connector-jdbc` / ClickHouse JDBC | tbd | Phase 3 |

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

See [docs/heartbeat-findings.md](docs/heartbeat-findings.md) for the heartbeat experiment results.

## Note

Credentials in this repo (for example `cdc_user` / `cdc_pass`) are local demo placeholders only.
