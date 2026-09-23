package com.example.cdc.multi;

import java.util.HashMap;
import java.util.Map;

/**
 * One change to ANY table in a slot group, in generic form: column name -> Java value,
 * built purely from the JSON in the Debezium envelope (no per-table Java class).
 *
 * Values come from Jackson's JSON type inference (String, BigDecimal, Boolean, or null),
 * not from Debezium's schema block -- this job runs with includeSchema=false to keep
 * messages small, so type authority (is this really an Int32 or a Int64? a Decimal or a
 * plain number?) is Postgres's and ClickHouse's, established once at DDL-generation time
 * (see scripts/generate_clickhouse_ddl.py), not re-derived from every message.
 */
public class GenericChange {

    public String table;
    public final Map<String, Object> fields = new HashMap<>();

    public char op;
    public long lsn;
    public long sourceTsMs;
    public long ingestTsMs;
    public byte isDeleted;

    public GenericChange() {}

    @Override
    public String toString() {
        return "GenericChange{table=" + table + ", op=" + op + ", fields=" + fields
                + ", lsn=" + lsn + ", isDeleted=" + isDeleted + "}";
    }
}
