package com.example.cdc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.example.cdc.model.ResourceChange;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class DebeziumJsonParserTest {

    private static final String ROW =
            "{\"resource_id\":\"res-1\",\"account_id\":\"a1\",\"region\":\"us-east-1\","
                    + "\"resource_type\":\"ec2\",\"instance_size\":null,\"team\":\"data\","
                    + "\"state\":\"%s\",\"hourly_cost\":1.2500,"
                    + "\"updated_at\":\"2026-09-20T01:57:10.228786Z\"}";

    private static String envelope(String op, String before, String after, long lsn, long tsMs) {
        return "{\"before\":" + before + ",\"after\":" + after
                + ",\"source\":{\"lsn\":" + lsn + ",\"ts_ms\":" + tsMs + ",\"table\":\"resource_inventory\"},"
                + "\"op\":\"" + op + "\"}";
    }

    private final DebeziumJsonParser parser = new DebeziumJsonParser();

    @Test
    void updateReadsAfterImageAndSourceMetadata() throws Exception {
        ResourceChange c = parser.map(envelope("u", ROW.formatted("running"), ROW.formatted("stopped"), 26773072L, 1789869430230L));
        assertEquals('u', c.op);
        assertEquals("stopped", c.state);
        assertEquals(26773072L, c.lsn);
        assertEquals(1789869430230L, c.sourceTsMs);
        assertEquals(0, c.isDeleted);
        assertEquals(0, new BigDecimal("1.25").compareTo(c.hourlyCost)); // value, not scale
        assertEquals(1789869430228L, c.updatedAt);
        assertNull(c.instanceSize);
    }

    @Test
    void deleteKeepsOriginalValuesAndSetsTombstone() throws Exception {
        ResourceChange c = parser.map(envelope("d", ROW.formatted("stopped"), "null", 26773472L, 1789869440000L));
        assertEquals('d', c.op);
        assertEquals(1, c.isDeleted);
        assertEquals("res-1", c.resourceId);
        assertEquals("stopped", c.state);
        assertEquals(26773472L, c.lsn);
    }

    @Test
    void snapshotRowGetsLowestVersion() throws Exception {
        ResourceChange c = parser.map(envelope("r", "null", ROW.formatted("running"), 0L, 0L));
        assertEquals('r', c.op);
        assertEquals(DebeziumJsonParser.SNAPSHOT_LSN, c.lsn);
        assertEquals(0L, c.sourceTsMs);
    }

    @Test
    void deleteWithoutBeforeImageFailsLoudly() {
        assertThrows(IllegalStateException.class,
                () -> parser.map(envelope("d", "null", "null", 5L, 5L)));
    }
}
