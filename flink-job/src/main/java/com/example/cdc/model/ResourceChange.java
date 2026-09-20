package com.example.cdc.model;

import java.math.BigDecimal;

/**
 * One change to resource_inventory, ready for the sink.
 *
 * Shaped as a Flink POJO (public class, public no-arg constructor, public non-final fields)
 * so Flink uses its fast PojoSerializer instead of falling back to Kryo.
 */
public class ResourceChange {

    // business columns
    public String resourceId;
    public String accountId;
    public String region;
    public String resourceType;
    public String instanceSize;
    public String team;
    public String state;
    public BigDecimal hourlyCost;
    /** Epoch millis. */
    public long updatedAt;

    // CDC metadata
    /** Debezium op: 'r' snapshot read, 'c' create, 'u' update, 'd' delete. */
    public char op;
    /** Postgres WAL position of the change; the row version. 0 for snapshot rows. */
    public long lsn;
    /** When the change committed in Postgres (epoch millis). 0 for snapshot rows. */
    public long sourceTsMs;
    /** When Flink processed the change (epoch millis). */
    public long ingestTsMs;
    /** 1 for a delete tombstone, else 0. */
    public byte isDeleted;

    public ResourceChange() {}

    @Override
    public String toString() {
        return "ResourceChange{op=" + op
                + ", resourceId=" + resourceId
                + ", accountId=" + accountId
                + ", region=" + region
                + ", resourceType=" + resourceType
                + ", instanceSize=" + instanceSize
                + ", team=" + team
                + ", state=" + state
                + ", hourlyCost=" + hourlyCost
                + ", updatedAt=" + updatedAt
                + ", lsn=" + lsn
                + ", sourceTsMs=" + sourceTsMs
                + ", ingestTsMs=" + ingestTsMs
                + ", isDeleted=" + isDeleted + "}";
    }
}
