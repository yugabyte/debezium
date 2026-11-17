/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;

/**
 * The replication-slot flush position must be derived from offsets Kafka Connect has actually
 * committed, never from the per-record {@code commitRecord} acknowledgement callback.
 *
 * Producer acknowledgements carry no ordering guarantee across topic-partitions, and records
 * dropped by an SMT are acknowledged without ever reaching Kafka. A flush position taken from that
 * callback can therefore move past changes that were never durably written, which is silent data
 * loss on connector restart. See DBZ-7816.
 */
public class PostgresConnectorTaskCommitTest {

    private static final Map<String, String> PARTITION = Collections.singletonMap("server", "srv_0_slot");

    private static final long DURABLE_LSN = 100L;
    private static final long ACKED_LSN = 999L;
    private static final long FILTERED_LSN = 1000L;

    private static Map<String, Object> offsetAt(long lsn) {
        Map<String, Object> offset = new HashMap<>();
        offset.put("lsn", lsn);
        offset.put("lsn_commit", lsn);
        return offset;
    }

    private static SourceRecord recordAt(long lsn) {
        return new SourceRecord(PARTITION, offsetAt(lsn), "topic", null, Schema.STRING_SCHEMA, "payload");
    }

    /** A task whose offset store is a canned value, so neither Kafka nor a database is needed. */
    private static class TestableTask extends PostgresConnectorTask {

        private final Offsets<PostgresPartition, PostgresOffsetContext> committed;

        TestableTask(Offsets<PostgresPartition, PostgresOffsetContext> committed,
                     ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> coordinator) {
            this.committed = committed;
            this.coordinator = coordinator;
        }

        @Override
        protected Offsets<PostgresPartition, PostgresOffsetContext> getPreviousOffsets(
                                                                                       Partition.Provider<PostgresPartition> provider,
                                                                                       OffsetContext.Loader<PostgresOffsetContext> loader) {
            return committed;
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void flushPositionMustComeFromCommittedOffsetsNotFromAcknowledgements() throws Exception {
        // The offset store says everything up to DURABLE_LSN is safely in Kafka.
        PostgresOffsetContext durable = mock(PostgresOffsetContext.class);
        when(durable.getOffset()).thenReturn((Map) offsetAt(DURABLE_LSN));

        ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> coordinator = mock(ChangeEventSourceCoordinator.class);
        TestableTask task = new TestableTask(
                Offsets.of(new PostgresPartition("srv", "db", "0", "slot"), durable), coordinator);

        // A later record is acknowledged by the broker while earlier ones are still in flight...
        task.commitRecord(recordAt(ACKED_LSN));
        // ...and a later one still is dropped by an SMT, so it never reaches Kafka at all.
        task.commitRecord(recordAt(FILTERED_LSN), null);

        task.commit();

        ArgumentCaptor<Map<String, ?>> flushed = ArgumentCaptor.forClass(Map.class);
        verify(coordinator).commitOffset(any(), flushed.capture());

        assertThat(flushed.getValue().get("lsn"))
                .as("flush position must be the durable committed offset, not an acknowledgement callback")
                .isEqualTo(DURABLE_LSN);
    }
}
