/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.function.BlockingConsumer;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.HeartbeatErrorHandler;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.schema.SchemaNameAdjuster;

/**
 * Unit tests for the YB-specific snapshot/streaming gating in
 * {@link YBDatabaseHeartbeatImpl}, plus verification that the action query is
 * (or is not) executed on each phase.
 */
public class YBDatabaseHeartbeatImplTest {

    private static final String TOPIC = "__debezium-heartbeat.test";
    private static final String KEY = "test_server";
    private static final String ACTION_QUERY = "INSERT INTO heartbeat VALUES (now())";

    @Mock
    private JdbcConnection jdbcConnection;

    @Mock
    private HeartbeatErrorHandler errorHandler;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    private static Map<String, Object> partition() {
        return Collections.singletonMap("server", KEY);
    }

    private static Map<String, Object> snapshotOffset() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(AbstractSourceInfo.SNAPSHOT_KEY, Boolean.TRUE);
        offset.put("lsn", 1L);
        return offset;
    }

    private static Map<String, Object> streamingOffset() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(AbstractSourceInfo.SNAPSHOT_KEY, Boolean.FALSE);
        offset.put("lsn", 1L);
        return offset;
    }

    private YBDatabaseHeartbeatImpl heartbeatWithInterval(Duration interval) {
        return new YBDatabaseHeartbeatImpl(
                interval,
                TOPIC,
                KEY,
                jdbcConnection,
                ACTION_QUERY,
                errorHandler,
                SchemaNameAdjuster.NO_OP);
    }

    private static final class RecordingConsumer implements BlockingConsumer<SourceRecord> {
        final List<SourceRecord> records = new ArrayList<>();

        @Override
        public void accept(SourceRecord record) {
            records.add(record);
        }
    }

    // ---------- zero-interval no-op (both heartbeat overloads) ----------

    @Test
    public void shouldNotHeartbeatOrRunQueryWhenIntervalIsZero() throws InterruptedException, SQLException {
        YBDatabaseHeartbeatImpl heartbeat = heartbeatWithInterval(Duration.ZERO);
        RecordingConsumer consumer = new RecordingConsumer();

        heartbeat.heartbeat(partition(), streamingOffset(), consumer);

        assertThat(consumer.records).isEmpty();
        verifyZeroInteractions(jdbcConnection);
    }

    @Test
    public void shouldNotHeartbeatOrRunQueryWhenIntervalIsZero_offsetProducer() throws InterruptedException, SQLException {
        YBDatabaseHeartbeatImpl heartbeat = heartbeatWithInterval(Duration.ZERO);
        RecordingConsumer consumer = new RecordingConsumer();

        boolean[] producerCalled = { false };
        Heartbeat.OffsetProducer producer = () -> {
            producerCalled[0] = true;
            return streamingOffset();
        };

        heartbeat.heartbeat(partition(), producer, consumer);

        assertThat(consumer.records).isEmpty();
        assertThat(producerCalled[0]).isFalse();
        verifyZeroInteractions(jdbcConnection);
    }

    // ---------- snapshot-phase no-op ----------

    @Test
    public void shouldNotHeartbeatOrRunQueryWhileInSnapshot() throws InterruptedException, SQLException {
        YBDatabaseHeartbeatImpl heartbeat = heartbeatWithInterval(Duration.ofMillis(100));
        RecordingConsumer consumer = new RecordingConsumer();

        heartbeat.heartbeat(partition(), snapshotOffset(), consumer);

        assertThat(consumer.records).isEmpty();
        verify(jdbcConnection, never()).execute(eq(ACTION_QUERY));
    }

    @Test
    public void shouldNotHeartbeatOrRunQueryWhileInSnapshot_offsetProducer() throws InterruptedException, SQLException {
        YBDatabaseHeartbeatImpl heartbeat = heartbeatWithInterval(Duration.ofMillis(100));
        RecordingConsumer consumer = new RecordingConsumer();

        heartbeat.heartbeat(partition(), YBDatabaseHeartbeatImplTest::snapshotOffset, consumer);

        assertThat(consumer.records).isEmpty();
        verify(jdbcConnection, never()).execute(eq(ACTION_QUERY));
    }

    // ---------- streaming-phase delegation: timer expires → record + action query ----------

    // Threads.Timer uses millisecond resolution and {@code expired()} returns {@code elapsed > intervalMs}.
    // Use a small non-zero interval plus a sleep that comfortably exceeds it so the timer is reliably
    // expired by the time {@code heartbeat(...)} is called.
    private static final Duration STREAMING_INTERVAL = Duration.ofMillis(1);
    private static final long SLEEP_PAST_INTERVAL_MS = 20L;

    @Test
    public void shouldEmitRecordAndRunActionQueryOnceTimerExpires() throws InterruptedException, SQLException {
        YBDatabaseHeartbeatImpl heartbeat = heartbeatWithInterval(STREAMING_INTERVAL);
        RecordingConsumer consumer = new RecordingConsumer();

        Thread.sleep(SLEEP_PAST_INTERVAL_MS);
        heartbeat.heartbeat(partition(), streamingOffset(), consumer);

        assertThat(consumer.records).hasSize(1);
        assertThat(consumer.records.get(0).topic()).isEqualTo(TOPIC);
        verify(jdbcConnection, times(1)).execute(eq(ACTION_QUERY));
    }

    @Test
    public void shouldEmitRecordAndRunActionQueryOnceTimerExpires_offsetProducer() throws InterruptedException, SQLException {
        YBDatabaseHeartbeatImpl heartbeat = heartbeatWithInterval(STREAMING_INTERVAL);
        RecordingConsumer consumer = new RecordingConsumer();

        boolean[] producerCalled = { false };
        Heartbeat.OffsetProducer producer = () -> {
            producerCalled[0] = true;
            return streamingOffset();
        };

        Thread.sleep(SLEEP_PAST_INTERVAL_MS);
        heartbeat.heartbeat(partition(), producer, consumer);

        assertThat(producerCalled[0]).isTrue();
        assertThat(consumer.records).hasSize(1);
        verify(jdbcConnection, times(1)).execute(eq(ACTION_QUERY));
    }

    // ---------- forcedBeat: inherited path used at snapshot-to-streaming transition ----------

    @Test
    public void forcedBeatShouldEmitRecordAndRunActionQueryEvenWithSnapshotOffset() throws InterruptedException, SQLException {
        // The transition wait calls forcedBeat() with an offset that still says snapshot;
        // the inherited DatabaseHeartbeatImpl.forcedBeat() must run the action query and emit a record.
        YBDatabaseHeartbeatImpl heartbeat = heartbeatWithInterval(Duration.ofMillis(100));
        RecordingConsumer consumer = new RecordingConsumer();

        heartbeat.forcedBeat(partition(), snapshotOffset(), consumer);

        assertThat(consumer.records).hasSize(1);
        verify(jdbcConnection, times(1)).execute(eq(ACTION_QUERY));
    }

    @Test
    public void forcedBeatShouldSwallowSqlExceptionAndStillEmitRecord() throws InterruptedException, SQLException {
        // Action-query failures are logged + forwarded to the error handler; the heartbeat
        // record itself must still be produced so the slot can advance.
        when(jdbcConnection.execute(eq(ACTION_QUERY))).thenThrow(new SQLException("boom", "08000"));

        YBDatabaseHeartbeatImpl heartbeat = heartbeatWithInterval(Duration.ofMillis(100));
        RecordingConsumer consumer = new RecordingConsumer();

        heartbeat.forcedBeat(partition(), streamingOffset(), consumer);

        assertThat(consumer.records).hasSize(1);
        verify(errorHandler, times(1)).onError(org.mockito.ArgumentMatchers.any(SQLException.class));
    }
}
