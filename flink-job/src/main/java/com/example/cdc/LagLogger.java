package com.example.cdc;

import com.example.cdc.model.ResourceChange;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pass-through that logs end-to-end lag (ingest time minus Postgres commit time) every N
 * streamed records. Snapshot rows are skipped: they have no commit timestamp.
 */
public class LagLogger implements MapFunction<ResourceChange, ResourceChange> {

    private static final Logger LOG = LoggerFactory.getLogger(LagLogger.class);

    private final int every;
    private long seen;

    public LagLogger(int every) {
        this.every = every;
    }

    @Override
    public ResourceChange map(ResourceChange c) {
        if (c.sourceTsMs > 0 && ++seen % every == 0) {
            LOG.info("CDC lag: {} ms (op={}, lsn={}, streamed={})",
                    c.ingestTsMs - c.sourceTsMs, c.op, c.lsn, seen);
        }
        return c;
    }
}
