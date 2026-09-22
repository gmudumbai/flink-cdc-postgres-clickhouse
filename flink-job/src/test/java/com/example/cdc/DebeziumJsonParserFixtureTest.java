package com.example.cdc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.example.cdc.model.ResourceChange;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.Test;

/**
 * Uses real Debezium envelopes captured from a live run of this pipeline
 * (Flink CDC 3.6.0-2.2, decimal.format=NUMERIC), not hand-written JSON.
 * Fixtures: src/test/resources/debezium-{create,update,delete}.json.
 */
class DebeziumJsonParserFixtureTest {

    private final DebeziumJsonParser parser = new DebeziumJsonParser();

    private static String fixture(String name) throws IOException {
        try (InputStream in = DebeziumJsonParserFixtureTest.class
                .getClassLoader().getResourceAsStream("debezium-" + name + ".json")) {
            if (in == null) {
                throw new IOException("fixture not found: debezium-" + name + ".json");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void create() throws Exception {
        ResourceChange c = parser.map(fixture("create"));
        assertEquals('c', c.op);
        assertEquals("fixture-2", c.resourceId);
        assertEquals("running", c.state);
        assertEquals(0, new BigDecimal("0.75").compareTo(c.hourlyCost));
        assertEquals(28008528L, c.lsn);
        assertEquals(1790041811583L, c.sourceTsMs);
        assertEquals(0, c.isDeleted);
        assertNull(c.instanceSize);
    }

    @Test
    void update() throws Exception {
        ResourceChange c = parser.map(fixture("update"));
        assertEquals('u', c.op);
        assertEquals("stopped", c.state);            // after image, not before
        assertEquals(0, new BigDecimal("1.5").compareTo(c.hourlyCost));
        assertEquals(28017232L, c.lsn);
        assertEquals(0, c.isDeleted);
    }

    @Test
    void delete() throws Exception {
        ResourceChange c = parser.map(fixture("delete"));
        assertEquals('d', c.op);
        assertEquals(1, c.isDeleted);
        // the row's last known values, not nulls -- this is the point of the test
        assertEquals("fixture-2", c.resourceId);
        assertEquals("stopped", c.state);
        assertEquals(0, new BigDecimal("1.5").compareTo(c.hourlyCost));
        assertEquals(28026008L, c.lsn);
    }

    @Test
    void fixturesMatchOneCommittedLifecycle() throws Exception {
        // Sanity check on the fixtures themselves: same key, strictly increasing LSN,
        // update's before == create's after, delete's before == update's after.
        ObjectMapper om = new ObjectMapper();
        JsonNode create = om.readTree(fixture("create"));
        JsonNode update = om.readTree(fixture("update"));
        JsonNode delete = om.readTree(fixture("delete"));

        assertEquals(create.path("after"), update.path("before"));
        assertEquals(update.path("after"), delete.path("before"));
        long lsnC = create.path("source").path("lsn").asLong();
        long lsnU = update.path("source").path("lsn").asLong();
        long lsnD = delete.path("source").path("lsn").asLong();
        assertEquals(true, lsnC < lsnU && lsnU < lsnD, "LSNs must strictly increase: " + lsnC + " < " + lsnU + " < " + lsnD);

        // updated_at is a timestamptz string; confirm it actually parses as one (Postgres format check)
        OffsetDateTime.parse(create.path("after").path("updated_at").asText());
    }
}
