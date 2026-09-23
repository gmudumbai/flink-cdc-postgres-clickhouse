package com.example.cdc.multi;

import com.clickhouse.data.ClickHouseColumn;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;

/**
 * The table-agnostic counterpart of com.example.cdc.ResourceChangeMapper: one instance
 * per table, but the SAME class -- built entirely from a TableMapping loaded from config,
 * never from a hand-written field list.
 */
public class GenericClickHouseMapper extends DataMapper<GenericChange> {

    private final TableMapping mapping;
    private final List<ColumnBinding> bindings;

    public GenericClickHouseMapper(TableMapping mapping) {
        this.mapping = mapping;
        List<ColumnBinding> b = new ArrayList<>();
        for (TableMapping.ColumnSpec col : mapping.columns) {
            // ColumnBinding.of() takes a raw ClickHouse type string, so the exact type text the
            // DDL generator wrote into the table (e.g. "Nullable(Int32)") is reused verbatim here.
            b.add(ColumnBinding.of(col.pgColumn, col.chColumn, ClickHouseColumn.of(col.chColumn, col.chType)));
        }
        b.add(ColumnBinding.of("_version", "_version", ClickHouseColumn.of("_version", "UInt64")));
        b.add(ColumnBinding.of("_is_deleted", "_is_deleted", ClickHouseColumn.of("_is_deleted", "UInt8")));
        this.bindings = List.copyOf(b);
    }

    @Override
    public void toMap(GenericChange c, Map<String, Object> m) {
        for (TableMapping.ColumnSpec col : mapping.columns) {
            Object v = c.fields.get(col.pgColumn);
            m.put(col.pgColumn, coerce(v, col.chType));
        }
        m.put("_version", c.lsn);
        m.put("_is_deleted", (int) c.isDeleted);
    }

    @Override
    public List<ColumnBinding> bindings() {
        return bindings;
    }

    /**
     * The generic parser hands back plain JSON-derived types (Boolean, Long, BigDecimal,
     * String, OffsetDateTime); this converts the two that the ClickHouse client won't accept
     * as-is. Driven by the DECLARED ClickHouse type, not by a per-table if/else -- this is
     * the one piece of type-specific logic the generic path still needs.
     */
    private static Object coerce(Object v, String chType) {
        if (v == null) return null;
        if (v instanceof Boolean b) {
            return b ? 1 : 0;                                    // UInt8 has no boolean literal
        }
        if (v instanceof OffsetDateTime dt && chType.contains("DateTime64")) {
            return dt.toInstant().atZone(ZoneOffset.UTC);         // what the CH client expects
        }
        // The generic parser only knows "JSON integral number", so it always hands back a Long;
        // the client library is strict about matching Java's boxed width to the declared CH width.
        if (v instanceof Long l) {
            if (chType.contains("Int64") || chType.contains("UInt64")) return l;
            if (chType.contains("Int32") || chType.contains("UInt32")) return l.intValue();
            if (chType.contains("Int16") || chType.contains("UInt16")) return l.shortValue();
            if (chType.contains("Int8") || chType.contains("UInt8")) return l.byteValue();
        }
        return v;
    }
}
