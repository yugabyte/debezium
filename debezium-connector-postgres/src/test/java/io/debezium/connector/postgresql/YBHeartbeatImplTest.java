/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.function.BlockingConsumer;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.schema.SchemaNameAdjuster;

/**
 * Unit tests for the YB-specific snapshot/streaming gating in {@link YBHeartbeatImpl}.
 *
 * Streaming-phase delegation is asserted via {@code forcedBeat}, which the
 * timer-driven path ultimately calls inside the upstream {@link io.debezium.heartbeat.HeartbeatImpl}.
 * The interval-expiry timing path is owned by upstream and is intentionally not retested here.
 */
public class YBHeartbeatImplTest {

    private static final String TOPIC = "__debezium-heartbeat.test";
    private static final String KEY = "test_server";

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

    private static YBHeartbeatImpl heartbeatWithInterval(Duration interval) {
        return new YBHeartbeatImpl(interval, TOPIC, KEY, SchemaNameAdjuster.NO_OP);
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
    public void shouldNotHeartbeatWhenIntervalIsZero() throws InterruptedException {
        YBHeartbeatImpl heartbeat = heartbeatWithInterval(Duration.ZERO);
        RecordingConsumer consumer = new RecordingConsumer();

        heartbeat.heartbeat(partition(), streamingOffset(), consumer);

        assertThat(consumer.records).isEmpty();
    }

    @Test
    public void shouldNotHeartbeatWhenIntervalIsZero_offsetProducer() throws InterruptedException {
        YBHeartbeatImpl heartbeat = heartbeatWithInterval(Duration.ZERO);
        RecordingConsumer consumer = new RecordingConsumer();

        // OffsetProducer should not even be invoked when interval is zero.
        boolean[] producerCalled = { false };
        Heartbeat.OffsetProducer producer = () -> {
            producerCalled[0] = true;
            return streamingOffset();
        };

        heartbeat.heartbeat(partition(), producer, consumer);

        assertThat(consumer.records).isEmpty();
        assertThat(producerCalled[0]).isFalse();
    }

    // ---------- snapshot-phase no-op ----------

    @Test
    public void shouldNotHeartbeatWhileInSnapshot() throws InterruptedException {
        YBHeartbeatImpl heartbeat = heartbeatWithInterval(Duration.ofMillis(100));
        RecordingConsumer consumer = new RecordingConsumer();

        heartbeat.heartbeat(partition(), snapshotOffset(), consumer);

        assertThat(consumer.records).isEmpty();
    }

    @Test
    public void shouldNotHeartbeatWhileInSnapshot_offsetProducer() throws InterruptedException {
        YBHeartbeatImpl heartbeat = heartbeatWithInterval(Duration.ofMillis(100));
        RecordingConsumer consumer = new RecordingConsumer();

        heartbeat.heartbeat(partition(), YBHeartbeatImplTest::snapshotOffset, consumer);

        assertThat(consumer.records).isEmpty();
    }

    // ---------- isInSnapshot helper ----------

    @Test
    public void isInSnapshotShouldHandleAllOffsetShapes() {
        assertThat(YBHeartbeatImpl.isInSnapshot(null)).isFalse();
        assertThat(YBHeartbeatImpl.isInSnapshot(Collections.emptyMap())).isFalse();

        Map<String, Object> snapshotFalse = new HashMap<>();
        snapshotFalse.put(AbstractSourceInfo.SNAPSHOT_KEY, Boolean.FALSE);
        assertThat(YBHeartbeatImpl.isInSnapshot(snapshotFalse)).isFalse();

        // Non-boolean values must not be treated as in-snapshot.
        Map<String, Object> snapshotString = new HashMap<>();
        snapshotString.put(AbstractSourceInfo.SNAPSHOT_KEY, "true");
        assertThat(YBHeartbeatImpl.isInSnapshot(snapshotString)).isFalse();

        Map<String, Object> snapshotTrue = new HashMap<>();
        snapshotTrue.put(AbstractSourceInfo.SNAPSHOT_KEY, Boolean.TRUE);
        assertThat(YBHeartbeatImpl.isInSnapshot(snapshotTrue)).isTrue();
    }

    // ---------- forcedBeat: unchanged in this PR, still works in every phase ----------

    @Test
    public void forcedBeatShouldEmitDuringStreaming() throws InterruptedException {
        YBHeartbeatImpl heartbeat = heartbeatWithInterval(Duration.ofMillis(100));
        RecordingConsumer consumer = new RecordingConsumer();

        heartbeat.forcedBeat(partition(), streamingOffset(), consumer);

        assertThat(consumer.records).hasSize(1);
        SourceRecord record = consumer.records.get(0);
        assertThat(record.topic()).isEqualTo(TOPIC);
        assertThat(record.sourcePartition()).isEqualTo(partition());
        assertThat(record.sourceOffset()).isEqualTo(streamingOffset());
    }

    @Test
    public void forcedBeatShouldEmitDuringSnapshotTransition() throws InterruptedException {
        // forcedBeat() is what the connector uses during the snapshot-to-streaming
        // transition wait; the snapshot gate in heartbeat() must not apply here.
        YBHeartbeatImpl heartbeat = heartbeatWithInterval(Duration.ofMillis(100));
        RecordingConsumer consumer = new RecordingConsumer();

        heartbeat.forcedBeat(partition(), snapshotOffset(), consumer);

        assertThat(consumer.records).hasSize(1);
        assertThat(consumer.records.get(0).sourceOffset()).isEqualTo(snapshotOffset());
    }

    @Test
    public void forcedBeatShouldStillWorkWithZeroInterval() throws InterruptedException {
        // Snapshot-to-streaming transition uses forcedBeat() regardless of heartbeat.interval.ms.
        YBHeartbeatImpl heartbeat = heartbeatWithInterval(Duration.ZERO);
        RecordingConsumer consumer = new RecordingConsumer();

        heartbeat.forcedBeat(partition(), streamingOffset(), consumer);

        assertThat(consumer.records).hasSize(1);
    }

    // ---------- streaming-phase delegation reaches the timer-driven path ----------

    // Threads.Timer uses millisecond resolution and {@code expired()} returns {@code elapsed > intervalMs}.
    // Use a small non-zero interval plus a sleep that comfortably exceeds it so the timer is reliably
    // expired by the time {@code heartbeat(...)} is called.
    private static final Duration STREAMING_INTERVAL = Duration.ofMillis(1);
    private static final long SLEEP_PAST_INTERVAL_MS = 20L;

    @Test
    public void shouldEmitRecordOnceTimerExpires() throws InterruptedException {
        YBHeartbeatImpl heartbeat = heartbeatWithInterval(STREAMING_INTERVAL);
        RecordingConsumer consumer = new RecordingConsumer();

        Thread.sleep(SLEEP_PAST_INTERVAL_MS);
        heartbeat.heartbeat(partition(), streamingOffset(), consumer);

        assertThat(consumer.records).hasSize(1);
        assertThat(consumer.records.get(0).topic()).isEqualTo(TOPIC);
    }

    @Test
    public void shouldEmitRecordOnceTimerExpires_offsetProducer() throws InterruptedException {
        YBHeartbeatImpl heartbeat = heartbeatWithInterval(STREAMING_INTERVAL);
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
    }
}
