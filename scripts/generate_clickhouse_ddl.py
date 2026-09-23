#!/usr/bin/env python3
"""Generates ClickHouse DDL (mirror table + current-state view) from a Postgres table's
actual schema, via information_schema -- so onboarding a new table to the pipeline doesn't
require hand-writing its ClickHouse counterpart.

Usage:
  scripts/generate_clickhouse_ddl.py TABLE [TABLE...]              # print DDL
  scripts/generate_clickhouse_ddl.py --apply TABLE [TABLE...]      # print, then execute

Prints before applying, always -- review it. This is a prototype: it round-trips the common
scalar types this project uses; it does not attempt arrays, JSON, enums, or TOAST-affected
types, and it assumes a single-column primary key.
"""
import argparse
import subprocess
import sys
import urllib.request

PG_CONTAINER = "cdc-postgres"
PG_DB = "inventory"
CH_URL = "http://localhost:8123/"
CH_AUTH = {"user": "default", "password": "clickhouse"}

# Postgres type -> ClickHouse type. Numeric precision/scale and character length are
# handled separately since they need the column's own metadata, not just its base type name.
PG_TO_CH = {
    "text": "String",
    "character varying": "String",
    "boolean": "UInt8",
    "integer": "Int32",
    "bigint": "Int64",
    "smallint": "Int16",
    "timestamp with time zone": "DateTime64(3)",
    "timestamp without time zone": "DateTime64(3)",
    "date": "Date",
}


def psql(sql):
    out = subprocess.run(
        ["docker", "exec", PG_CONTAINER, "psql", "-U", "postgres", "-d", PG_DB, "-tA", "-F", "|", "-c", sql],
        capture_output=True, text=True, check=True).stdout
    return [line.split("|") for line in out.strip().splitlines() if line.strip()]


def primary_key(table):
    rows = psql(f"""
        SELECT a.attname FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = '{table}'::regclass AND i.indisprimary""")
    if len(rows) != 1:
        raise SystemExit(f"{table}: expected exactly one primary key column, found {len(rows)}")
    return rows[0][0]


def columns(table):
    rows = psql(f"""
        SELECT column_name, data_type, is_nullable, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_name = '{table}' ORDER BY ordinal_position""")
    return [{"name": r[0], "pg_type": r[1], "nullable": r[2] == "YES",
              "precision": r[3] or None, "scale": r[4] or None} for r in rows]


def ch_type(col):
    if col["pg_type"] == "numeric":
        p = col["precision"] or 18
        s = col["scale"] or 4
        base = f"Decimal({p}, {s})"
    elif col["pg_type"] not in PG_TO_CH:
        raise SystemExit(f"no ClickHouse mapping for Postgres type '{col['pg_type']}' (column {col['name']})")
    else:
        base = PG_TO_CH[col["pg_type"]]
    return f"Nullable({base})" if col["nullable"] else base


def generate(table):
    pk = primary_key(table)
    cols = columns(table)
    lines = [f"CREATE TABLE cdc.{table} ("]
    for c in cols:
        lines.append(f"  {c['name']:<16} {ch_type(c)},")
    lines.append("  _version       UInt64,")
    lines.append("  _is_deleted    UInt8")
    lines.append(f") ENGINE = ReplacingMergeTree(_version, _is_deleted)")
    lines.append(f"ORDER BY {pk};")
    lines.append("")
    lines.append(f"CREATE VIEW cdc.{table}_current AS")
    lines.append(f"SELECT * FROM cdc.{table} FINAL WHERE _is_deleted = 0;")
    lines.append("")
    lines.append(f"GRANT INSERT ON cdc.{table} TO flink_sink;")
    return "\n".join(lines)


def generate_mapping(table):
    """Same introspection, shaped as the Java job's generic-sink config: one JSON entry
    per table, column type strings identical to what generate() put in the DDL -- the
    DDL and the Java mapping come from the same source of truth, not two hand-kept copies."""
    cols = columns(table)
    return {
        "pgTable": table,
        "chTable": table,
        "columns": [{"pgColumn": c["name"], "chColumn": c["name"], "chType": ch_type(c)} for c in cols],
    }


def apply(ddl):
    for stmt in ddl.split(";\n"):
        stmt = stmt.strip()
        if not stmt:
            continue
        req = urllib.request.Request(
            CH_URL + "?" + "&".join(f"{k}={v}" for k, v in CH_AUTH.items()),
            data=(stmt + ";").encode(), method="POST")
        with urllib.request.urlopen(req) as resp:
            resp.read()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("tables", nargs="+")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--mapping-out", help="write the Java-side generic-sink config as JSON to this file")
    a = ap.parse_args()
    tables = [t for t in a.tables if t != "--apply"]
    mappings = []
    for t in tables:
        ddl = generate(t)
        print(f"-- {t} " + "-" * (60 - len(t)))
        print(ddl)
        print()
        if a.apply:
            apply(ddl)
            print(f"-- applied {t}", file=sys.stderr)
        mappings.append(generate_mapping(t))
    if a.mapping_out:
        import json
        with open(a.mapping_out, "w") as f:
            json.dump(mappings, f, indent=2)
        print(f"-- wrote {a.mapping_out}", file=sys.stderr)


if __name__ == "__main__":
    main()
