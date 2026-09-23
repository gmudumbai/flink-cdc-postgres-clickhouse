package com.example.cdc.multi;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;

/**
 * One table's column-name-to-ClickHouse-type mapping, loaded from JSON produced by
 * scripts/generate_clickhouse_ddl.py --mapping-out. Adding a table to this pipeline means
 * adding one JSON entry (generated, not hand-written), not writing a new Java class.
 */
public class TableMapping implements Serializable {

    public String pgTable;
    public String chTable;
    public List<ColumnSpec> columns;

    public static class ColumnSpec implements Serializable {
        public String pgColumn;
        public String chColumn;
        public String chType;
    }

    public static List<TableMapping> loadAll(String resourcePath) throws IOException {
        try (InputStream in = TableMapping.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IOException("mapping config not found on classpath: " + resourcePath);
            }
            return new ObjectMapper().readValue(in,
                    new com.fasterxml.jackson.core.type.TypeReference<List<TableMapping>>() {});
        }
    }
}
