package com.example.cdc;

import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Phase 1: Postgres CDC source (snapshot, then log streaming) printing raw Debezium JSON.
 *
 * Args (all optional): --host --port --heartbeat-ms --checkpoint-ms --startup initial|latest
 */
public class CdcJob {

    public static void main(String[] args) throws Exception {
        String host = arg(args, "--host", "postgres");
        int port = Integer.parseInt(arg(args, "--port", "5432"));
        long heartbeatMs = Long.parseLong(arg(args, "--heartbeat-ms", "5000"));
        StartupOptions startup = "latest".equals(arg(args, "--startup", "initial"))
                ? StartupOptions.latest() : StartupOptions.initial();
        long checkpointMs = Long.parseLong(arg(args, "--checkpoint-ms", "10000"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // The incremental snapshot source finishes snapshot splits on checkpoints, so this is required.
        env.enableCheckpointing(checkpointMs);

        Properties debezium = new Properties();
        // Use the publication created in SQL; never let the connector create one (cdc_user lacks CREATE).
        debezium.setProperty("publication.name", "flink_pub");
        debezium.setProperty("publication.autocreate.mode", "disabled");
        // Heartbeat: without it a quiet published table means the slot never advances while the
        // rest of the database writes, and WAL piles up. The action query is a real write to a
        // published table, which forces a record the consumer can confirm. 0 disables it.
        debezium.setProperty("heartbeat.interval.ms", String.valueOf(heartbeatMs));
        if (heartbeatMs > 0) {
            debezium.setProperty("heartbeat.action.query",
                    "UPDATE cdc_heartbeat SET ts = now() WHERE id = 1");
        }

        var source = PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                .hostname(host)
                .port(port)
                .database("inventory")
                .schemaList("public")
                .tableList("public.resource_inventory")
                .username("cdc_user")
                .password("cdc_pass")
                .slotName("flink_slot")
                // pgoutput ships with Postgres, so the stock image works with no extra plugin.
                .decodingPluginName("pgoutput")
                .startupOptions(startup)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .debeziumProperties(debezium)
                // Parallelism > 1 only helps the snapshot phase (chunks read in parallel).
                // Streaming is single-reader: there is one replication slot to read from.
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "postgres-cdc")
                .setParallelism(2)
                .filter(new HeartbeatFilter())
                .print();

        env.execute("postgres-cdc-phase1");
    }

    private static String arg(String[] args, String name, String dflt) {
        for (int i = 0; i < args.length - 1; i++) {
            if (name.equals(args[i])) {
                return args[i + 1];
            }
        }
        return dflt;
    }
}
