package com.example.cdc;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.example.cdc.model.ResourceChange;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;

/**
 * Maps a {@link ResourceChange} onto the columns of cdc.resource_inventory.
 * Column names, order and types must match clickhouse/init/01-schema.sql.
 */
public class ResourceChangeMapper extends DataMapper<ResourceChange> {

    @Override
    public void toMap(ResourceChange c, Map<String, Object> m) {
        m.put("resource_id", c.resourceId);
        m.put("account_id", c.accountId);
        m.put("region", c.region);
        m.put("resource_type", c.resourceType);
        m.put("instance_size", c.instanceSize);   // nullable
        m.put("team", c.team);                    // nullable
        m.put("state", c.state);
        m.put("hourly_cost", c.hourlyCost);       // nullable
        m.put("updated_at", Instant.ofEpochMilli(c.updatedAt).atZone(ZoneOffset.UTC));
        m.put("_version", c.lsn);
        m.put("_is_deleted", (int) c.isDeleted);
    }

    @Override
    public List<ColumnBinding> bindings() {
        return List.of(
                ColumnBinding.scalar("resource_id", "resource_id", ClickHouseDataType.String),
                ColumnBinding.scalar("account_id", "account_id", ClickHouseDataType.String),
                ColumnBinding.scalar("region", "region", ClickHouseDataType.String),
                ColumnBinding.scalar("resource_type", "resource_type", ClickHouseDataType.String),
                ColumnBinding.scalar("instance_size", "instance_size", ClickHouseDataType.String, true, false),
                ColumnBinding.scalar("team", "team", ClickHouseDataType.String, true, false),
                ColumnBinding.scalar("state", "state", ClickHouseDataType.String),
                ColumnBinding.of("hourly_cost", "hourly_cost",
                        ClickHouseColumn.of("hourly_cost", "Nullable(Decimal(10, 4))")),
                ColumnBinding.dateTime64("updated_at", "updated_at", 3),
                ColumnBinding.scalar("_version", "_version", ClickHouseDataType.UInt64),
                ColumnBinding.scalar("_is_deleted", "_is_deleted", ClickHouseDataType.UInt8));
    }
}
