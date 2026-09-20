# flink-cdc-postgres-clickhouse

Change Data Capture from PostgreSQL into ClickHouse using Apache Flink CDC. An operational cloud resource inventory in Postgres is replicated into ClickHouse in near real time, so cost rollups by team, account and region run on the analytical store instead of the OLTP database.

```
Postgres (logical replication) ──> Flink CDC job (DataStream API) ──> ClickHouse (ReplacingMergeTree)
```

## Status

Planning stage. The full phase-by-phase implementation plan is in
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

Flink, Flink CDC, `flink-connector-jdbc` and the ClickHouse JDBC driver versions must line up. The working combination will be recorded here once verified.

## Quick start

Coming once Phase 0 (infrastructure) lands:

```bash
docker compose up -d
```

## Note

Credentials in this repo (for example `cdc_user` / `cdc_pass`) are local demo placeholders only.
