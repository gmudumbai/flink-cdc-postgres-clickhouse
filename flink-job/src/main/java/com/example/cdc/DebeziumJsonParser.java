package com.example.cdc;

import com.example.cdc.model.ResourceChange;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import org.apache.flink.api.common.functions.MapFunction;

/** Turns a Debezium change-event envelope (JSON string) into a {@link ResourceChange}. */
public class DebeziumJsonParser implements MapFunction<String, ResourceChange> {

    /**
     * Version given to snapshot rows. Real changes always have lsn > 0, so a snapshot row can
     * never beat a later change downstream.
     */
    static final long SNAPSHOT_LSN = 0L;

    private transient ObjectMapper mapper;

    @Override
    public ResourceChange map(String json) throws Exception {
        if (mapper == null) {
            // Keep NUMERIC values exact instead of rounding through double.
            mapper = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        }
        return parse(mapper.readTree(json));
    }

    ResourceChange parse(JsonNode envelope) {
        char op = envelope.path("op").asText().charAt(0);
        boolean isDelete = op == 'd';

        // Deletes carry the row in "before" (REPLICA IDENTITY FULL); everything else in "after".
        JsonNode row = envelope.path(isDelete ? "before" : "after");
        if (row.isMissingNode() || row.isNull()) {
            throw new IllegalStateException(
                    "op '" + op + "' has no " + (isDelete ? "before" : "after")
                            + " image; is REPLICA IDENTITY FULL set? " + envelope);
        }

        ResourceChange c = new ResourceChange();
        c.resourceId = text(row, "resource_id");
        c.accountId = text(row, "account_id");
        c.region = text(row, "region");
        c.resourceType = text(row, "resource_type");
        c.instanceSize = text(row, "instance_size");
        c.team = text(row, "team");
        c.state = text(row, "state");
        JsonNode cost = row.path("hourly_cost");
        c.hourlyCost = cost.isNull() || cost.isMissingNode() ? null : cost.decimalValue();
        JsonNode updated = row.path("updated_at");
        c.updatedAt = updated.isNull() || updated.isMissingNode()
                ? 0L : OffsetDateTime.parse(updated.asText()).toInstant().toEpochMilli();

        c.op = op;
        c.isDeleted = (byte) (isDelete ? 1 : 0);

        // Snapshot records have lsn 0/null and ts_ms 0 in the source block; treat them explicitly.
        JsonNode source = envelope.path("source");
        boolean snapshot = op == 'r';
        c.lsn = snapshot || source.path("lsn").isNull() ? SNAPSHOT_LSN : source.path("lsn").asLong(SNAPSHOT_LSN);
        c.sourceTsMs = snapshot ? 0L : source.path("ts_ms").asLong(0L);
        c.ingestTsMs = System.currentTimeMillis();
        return c;
    }

    private static String text(JsonNode row, String field) {
        JsonNode n = row.path(field);
        return n.isNull() || n.isMissingNode() ? null : n.asText();
    }
}
