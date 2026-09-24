package io.debezium.connector.postgresql;

import io.debezium.function.BlockingConsumer;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.heartbeat.HeartbeatErrorHandler;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.schema.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

/**
 * YugabyteDB specific database heartbeat implementation that allows the forcedBeat method
 * during the snapshot-to-streaming transition phase, and the regular heartbeat method only
 * during the streaming phase.
 *
 * @author bakul-gupta
 */
public class YBDatabaseHeartbeatImpl extends DatabaseHeartbeatImpl {

    private static final Logger LOGGER = LoggerFactory.getLogger(YBDatabaseHeartbeatImpl.class);

    private final Duration heartbeatInterval;
    private final long heartbeatLogIntervalMs;
    private long lastHeartbeatLogTimeMs = 0;

    public YBDatabaseHeartbeatImpl(Duration heartbeatInterval, Duration heartbeatLogInterval, String topicName, String key,
                                   JdbcConnection jdbcConnection, String heartBeatActionQuery, HeartbeatErrorHandler errorHandler,
                                   SchemaNameAdjuster schemaNameAdjuster) {
        super(heartbeatInterval, topicName, key, jdbcConnection, heartBeatActionQuery, errorHandler, schemaNameAdjuster);
        this.heartbeatInterval = heartbeatInterval;
        this.heartbeatLogIntervalMs = heartbeatLogInterval.toMillis();
    }

    @Override
    public void heartbeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        if (heartbeatInterval.isZero() || YBHeartbeatImpl.isInSnapshot(offset)) {
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
        if (YBHeartbeatImpl.isInSnapshot(resolved)) {
            return;
        }
        super.heartbeat(partition, () -> resolved, consumer);
    }

    @Override
    public void forcedBeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        super.forcedBeat(partition, offset, record -> {
            consumer.accept(record);
            maybeLogHeartbeat();
        });
    }

    private void maybeLogHeartbeat() {
        final long currentTimeMs = System.currentTimeMillis();
        if (lastHeartbeatLogTimeMs == 0L || currentTimeMs - lastHeartbeatLogTimeMs >= heartbeatLogIntervalMs) {
            LOGGER.info("Sent heartbeat record");
            lastHeartbeatLogTimeMs = currentTimeMs;
        }
    }
}
