package io.debezium.connector.postgresql;

import io.debezium.function.BlockingConsumer;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.heartbeat.HeartbeatErrorHandler;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.schema.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;

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

    private final Duration heartbeatInterval;

    public YBDatabaseHeartbeatImpl(Duration heartbeatInterval, String topicName, String key, JdbcConnection jdbcConnection,
                                   String heartBeatActionQuery, HeartbeatErrorHandler errorHandler,
                                   SchemaNameAdjuster schemaNameAdjuster) {
        super(heartbeatInterval, topicName, key, jdbcConnection, heartBeatActionQuery, errorHandler, schemaNameAdjuster);
        this.heartbeatInterval = heartbeatInterval;
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
}
