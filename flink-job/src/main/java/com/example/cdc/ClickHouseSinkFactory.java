package com.example.cdc;

import com.example.cdc.model.ResourceChange;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;

/**
 * Builds the ClickHouse sink (official connector, DataStream Sink V2).
 *
 * Always inserts: updates and deletes are new rows distinguished by _version and _is_deleted.
 * Delivery is at-least-once; a replayed change carries the same _version, so ReplacingMergeTree
 * collapses the duplicate. That makes the end result effectively-once without a transactional sink.
 */
public final class ClickHouseSinkFactory {

    private ClickHouseSinkFactory() {}

    public static ClickHouseAsyncSink<ResourceChange> create(
            String url, String username, String password, String database, String table) {
        ClickHouseClientConfig config = new ClickHouseClientConfig(url, username, password, database, table);
        return ClickHouseAsyncSink.<ResourceChange>builder()
                .setElementConverter(new ClickHouseConvertor<>(ResourceChange.class, new ResourceChangeMapper()))
                .setClickHouseClientConfig(config)
                // Every INSERT creates a part in ClickHouse; tiny batches cause "too many parts".
                // These trade latency (time in buffer) against part count (batch size).
                .setMaxBatchSize(500)
                .setMaxTimeInBufferMS(2000)
                .setMaxBatchSizeInBytes(1024 * 1024)
                .setMaxInFlightRequests(2)
                .setMaxBufferedRequests(5000)
                .setMaxRecordSizeInBytes(4096)
                .build();
    }
}
