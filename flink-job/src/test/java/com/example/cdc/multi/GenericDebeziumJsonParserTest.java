package com.example.cdc.multi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Real Debezium envelopes from TWO differently-shaped tables (network_interfaces:
 * booleans + nullable IP; snapshot_jobs: nullable integer + two independent timestamps),
 * parsed by the SAME GenericDebeziumJsonParser instance -- this is the proof that a
 * schema-driven parser needs no per-table code, unlike DebeziumJsonParser/ResourceChange.
 */
class GenericDebeziumJsonParserTest {

    private final GenericDebeziumJsonParser parser = new GenericDebeziumJsonParser();

    private static String fixture(String name) throws IOException {
        try (InputStream in = GenericDebeziumJsonParserTest.class.getClassLoader()
                .getResourceAsStream("multi/debezium-" + name + ".json")) {
            if (in == null) throw new IOException("fixture not found: " + name);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void networkInterfaceCreate_booleanAndNullField() throws Exception {
        GenericChange c = parser.map(fixture("network-interfaces-create"));
        assertEquals("network_interfaces", c.table);
        assertEquals('c', c.op);
        assertEquals(0, c.isDeleted);
        assertEquals("fixture-eni", c.fields.get("interface_id"));
        assertEquals(Boolean.TRUE, c.fields.get("is_primary"));   // JSON boolean -> Java Boolean
        assertNull(c.fields.get("public_ip"));                     // JSON null -> Java null
        assertTrue(c.fields.get("created_at") instanceof java.time.OffsetDateTime);
        assertEquals(26867128L, c.lsn);
    }

    @Test
    void networkInterfaceDelete_keepsLastKnownValues() throws Exception {
        GenericChange c = parser.map(fixture("network-interfaces-delete"));
        assertEquals('d', c.op);
        assertEquals(1, c.isDeleted);
        assertEquals("fixture-eni", c.fields.get("interface_id"));   // from before, not empty
        assertEquals(Boolean.TRUE, c.fields.get("is_primary"));
    }

    @Test
    void snapshotJobCreate_nullableIntegerAndTimestamp() throws Exception {
        GenericChange c = parser.map(fixture("snapshot-jobs-create"));
        assertEquals("snapshot_jobs", c.table);
        assertEquals('c', c.op);
        assertEquals(77L, c.fields.get("size_gb"));        // JSON integral -> Java Long
        assertNull(c.fields.get("completed_at"));           // still running: nullable timestamp is null
        assertEquals("running", c.fields.get("status"));
    }

    @Test
    void snapshotJobUpdate_completedAtNowPopulated() throws Exception {
        GenericChange c = parser.map(fixture("snapshot-jobs-update"));
        assertEquals('u', c.op);
        assertEquals("completed", c.fields.get("status"));
        assertTrue(c.fields.get("completed_at") instanceof java.time.OffsetDateTime);
        assertEquals(77L, c.fields.get("size_gb"));         // unchanged field still present after image
    }

    @Test
    void sameParserHandlesBothTableShapesWithNoTableSpecificCode() throws Exception {
        GenericChange eni = parser.map(fixture("network-interfaces-create"));
        GenericChange snap = parser.map(fixture("snapshot-jobs-create"));
        assertTrue(eni.fields.containsKey("mac_address") && !snap.fields.containsKey("mac_address"));
        assertTrue(snap.fields.containsKey("size_gb") && !eni.fields.containsKey("size_gb"));
    }
}
