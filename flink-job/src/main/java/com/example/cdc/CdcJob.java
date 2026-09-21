package com.example.cdc;

import com.example.cdc.model.ResourceChange;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Phase 3: Postgres CDC source (snapshot, then log streaming) -> typed ResourceChange -> ClickHouse.
 *
 * Args (all optional): --host --port --heartbeat-ms --checkpoint-ms --startup initial|latest
 * --clickhouse-url --clickhouse-user --clickhouse-password
 */
public class CdcJob {

    public static void main(String[] args) throws Exception {
        String host = arg(args, "--host", "postgres");
        int port = Integer.parseInt(arg(args, "--port", "5432"));
        long heartbeatMs = Long.parseLong(arg(args, "--heartbeat-ms", "5000"));
        StartupOptions startup = "latest".equals(arg(args, "--startup", "initial"))
                ? StartupOptions.latest() : StartupOptions.initial();
        String chUrl = arg(args, "--clickhouse-url", "http://clickhouse:8123");
        String chUser = arg(args, "--clickhouse-user", "flink_sink");
        String chPassword = arg(args, "--clickhouse-password", "flink_pass");
        long checkpointMs = Long.parseLong(arg(args, "--checkpoint-ms", "10000"));

        // Fixed-delay restart: after a failure (e.g. the TaskManager dies) retry 3 times, 5 s apart,
        // restoring from the latest completed checkpoint each time.
        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(5));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // Checkpoints go to the shared volume set in the cluster config (state.checkpoints.dir).
        // A checkpoint holds the source's WAL position (LSN) and snapshot progress, plus the sink's
        // buffered requests. The source confirms the LSN to the replication slot only after a
        // checkpoint completes, so Postgres never discards WAL the job could still need to replay.
        // The incremental snapshot source also finishes snapshot splits on checkpoints, so this is required.
        env.enableCheckpointing(checkpointMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3_000);
        // Keep the last checkpoint after cancel so a job can be resubmitted from it (no re-snapshot).
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

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
                // decimal.format=NUMERIC emits NUMERIC columns as JSON numbers, not base64 bytes.
                .deserializer(new JsonDebeziumDeserializationSchema(false, Map.of("decimal.format", "NUMERIC")))
                .debeziumProperties(debezium)
                // Parallelism > 1 only helps the snapshot phase (chunks read in parallel).
                // Streaming is single-reader: there is one replication slot to read from.
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "postgres-cdc")
                .setParallelism(2)
                .filter(new HeartbeatFilter())
                .map(new DebeziumJsonParser()).returns(ResourceChange.class)
                .map(new LagLogger(10))
                .sinkTo(ClickHouseSinkFactory.create(chUrl, chUser, chPassword, "cdc", "resource_inventory"));

        env.execute("postgres-cdc");
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
