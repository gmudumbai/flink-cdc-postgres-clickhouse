package com.example.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Drops changes to the heartbeat table so they never reach the business stream.
 * Filters on source.table in the Debezium envelope rather than guessing the payload shape.
 */
public class HeartbeatFilter implements FilterFunction<String> {

    static final String HEARTBEAT_TABLE = "cdc_heartbeat";

    private transient ObjectMapper mapper;

    @Override
    public boolean filter(String json) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        JsonNode table = mapper.readTree(json).path("source").path("table");
        return !HEARTBEAT_TABLE.equals(table.asText());
    }
}
