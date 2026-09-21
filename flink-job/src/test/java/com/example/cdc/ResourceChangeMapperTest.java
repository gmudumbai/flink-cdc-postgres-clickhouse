package com.example.cdc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.cdc.model.ResourceChange;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.junit.jupiter.api.Test;

class ResourceChangeMapperTest {

    private final ResourceChangeMapper mapper = new ResourceChangeMapper();

    private static ResourceChange row() {
        ResourceChange c = new ResourceChange();
        c.resourceId = "res-1"; c.accountId = "a1"; c.region = "us-east-1"; c.resourceType = "ec2";
        c.state = "running"; c.hourlyCost = new BigDecimal("1.25"); c.updatedAt = 1789946129313L;
        c.lsn = 42L; c.isDeleted = 1;
        return c;
    }

    @Test
    void everyBindingHasAMatchingMapKeyAndViceVersa() {
        Map<String, Object> m = new HashMap<>();
        mapper.toMap(row(), m);
        List<ColumnBinding> bindings = mapper.bindings();
        assertEquals(m.keySet(), bindings.stream().map(b -> b.mapKey).collect(java.util.stream.Collectors.toSet()));
        assertEquals(11, bindings.size());
    }

    @Test
    void versionAndTombstoneComeFromLsnAndIsDeleted() {
        Map<String, Object> m = new HashMap<>();
        mapper.toMap(row(), m);
        assertEquals(42L, m.get("_version"));
        assertEquals(1, m.get("_is_deleted"));
    }

    @Test
    void nullableColumnsPassNullsThrough() {
        Map<String, Object> m = new HashMap<>();
        ResourceChange c = row();
        c.hourlyCost = null;
        mapper.toMap(c, m);
        assertTrue(m.containsKey("team") && m.get("team") == null);
        assertTrue(m.containsKey("instance_size") && m.get("instance_size") == null);
        assertTrue(m.containsKey("hourly_cost") && m.get("hourly_cost") == null);
        for (ColumnBinding b : mapper.bindings()) {
            if (b.mapKey.equals("team") || b.mapKey.equals("instance_size") || b.mapKey.equals("hourly_cost")) {
                assertTrue(b.column.isNullable(), b.mapKey + " must be Nullable");
            }
        }
    }
}
