package io.debezium.connector.postgresql;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.function.BlockingConsumer;
import io.debezium.heartbeat.HeartbeatImpl;
import io.debezium.schema.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.Duration;
import java.util.Map;

/**
 * YugabyteDB specific heartbeat implementation that allows the forcedBeat method during the
 * snapshot-to-streaming transition phase, and the regular heartbeat method only during the
 * streaming phase.
 */
public class YBHeartbeatImpl extends HeartbeatImpl {

    private final Duration heartbeatInterval;

    public YBHeartbeatImpl(Duration heartbeatInterval, String topicName, String key, SchemaNameAdjuster schemaNameAdjuster) {
        super(heartbeatInterval, topicName, key, schemaNameAdjuster);
        this.heartbeatInterval = heartbeatInterval;
    }

    static boolean isInSnapshot(Map<String, ?> offset) {
        return offset != null && Boolean.TRUE.equals(offset.get(AbstractSourceInfo.SNAPSHOT_KEY));
    }

    @Override
    public void heartbeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        if (heartbeatInterval.isZero() || isInSnapshot(offset)) {
            return;
        }
        super.heartbeat(partition, offset, consumer);
    }

    @Override
    public void heartbeat(Map<String, ?> partition, OffsetProducer offsetProducer, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        if (heartbeatInterval.isZero()) {
            return;
        }
        Map<String, ?> resolved = offsetProducer.offset();
        if (isInSnapshot(resolved)) {
            return;
        }
        super.heartbeat(partition, () -> resolved, consumer);
    }

    @Override
    public void forcedBeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        super.forcedBeat(partition, offset, consumer);
    }
}
