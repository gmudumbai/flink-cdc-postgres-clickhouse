package com.example.cdc.multi;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Iterator;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * The table-agnostic counterpart of com.example.cdc.DebeziumJsonParser: same envelope
 * shape (before/after/source/op), but it walks whatever fields the row actually has
 * instead of a fixed list, so ONE class serves every table in the slot group.
 */
public class GenericDebeziumJsonParser implements MapFunction<String, GenericChange> {

    private transient ObjectMapper mapper;

    @Override
    public GenericChange map(String json) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        }
        JsonNode envelope = mapper.readTree(json);
        char op = envelope.path("op").asText().charAt(0);
        boolean isDelete = op == 'd';
        JsonNode row = envelope.path(isDelete ? "before" : "after");
        if (row.isMissingNode() || row.isNull()) {
            throw new IllegalStateException(
                    "op '" + op + "' has no " + (isDelete ? "before" : "after")
                            + " image; is REPLICA IDENTITY FULL set? " + envelope);
        }

        GenericChange c = new GenericChange();
        c.table = envelope.path("source").path("table").asText();
        c.op = op;
        c.isDeleted = (byte) (isDelete ? 1 : 0);

        for (Iterator<Map.Entry<String, JsonNode>> it = row.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            c.fields.put(e.getKey(), toJavaValue(e.getValue()));
        }

        JsonNode source = envelope.path("source");
        boolean snapshot = op == 'r';
        c.lsn = snapshot || source.path("lsn").isNull() ? 0L : source.path("lsn").asLong(0L);
        c.sourceTsMs = snapshot ? 0L : source.path("ts_ms").asLong(0L);
        c.ingestTsMs = System.currentTimeMillis();
        return c;
    }

    /** JSON -> Java, generically: no column-name knowledge, only JSON node types. */
    private static Object toJavaValue(JsonNode v) {
        if (v.isNull() || v.isMissingNode()) return null;
        if (v.isBoolean()) return v.asBoolean();
        if (v.isIntegralNumber()) return v.asLong();
        if (v.isBigDecimal() || v.isFloatingPointNumber()) return v.decimalValue();
        String s = v.asText();
        // Postgres timestamptz comes through as an ISO-8601 string; everything else stays a String.
        if (s.length() >= 20 && (s.endsWith("Z") || s.matches(".*[+-]\\d\\d:\\d\\d$"))) {
            try {
                return OffsetDateTime.parse(s);
            } catch (Exception ignored) {
                // not actually a timestamp -- fall through and keep it as text
            }
        }
        return s;
    }
}
