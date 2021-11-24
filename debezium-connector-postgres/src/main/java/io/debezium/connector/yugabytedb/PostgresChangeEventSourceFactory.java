/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.util.Optional;

import io.debezium.connector.yugabytedb.connection.PostgresConnection;
import io.debezium.connector.yugabytedb.connection.ReplicationConnection;
import io.debezium.connector.yugabytedb.spi.SlotCreationResult;
import io.debezium.connector.yugabytedb.spi.SlotState;
import io.debezium.connector.yugabytedb.spi.Snapshotter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;

public class PostgresChangeEventSourceFactory implements ChangeEventSourceFactory<PostgresPartition, PostgresOffsetContext> {

    private final PostgresConnectorConfig configuration;
    private final PostgresConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final PostgresSchema schema;
    private final PostgresTaskContext taskContext;
    private final Snapshotter snapshotter;
    private final ReplicationConnection replicationConnection;
    private final SlotCreationResult slotCreatedInfo;
    private final SlotState startingSlotInfo;

    public PostgresChangeEventSourceFactory(PostgresConnectorConfig configuration, Snapshotter snapshotter, PostgresConnection jdbcConnection,
                                            ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock, PostgresSchema schema,
                                            PostgresTaskContext taskContext,
                                            ReplicationConnection replicationConnection, SlotCreationResult slotCreatedInfo, SlotState startingSlotInfo) {
        this.configuration = configuration;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
        this.replicationConnection = replicationConnection;
        this.slotCreatedInfo = slotCreatedInfo;
        this.startingSlotInfo = startingSlotInfo;
    }

    @Override
    public SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener snapshotProgressListener) {
        return new PostgresSnapshotChangeEventSource(
                configuration,
                taskContext,
                snapshotter,
                schema,
                dispatcher,
                clock,
                snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource<PostgresPartition, PostgresOffsetContext> getStreamingChangeEventSource() {
        return new PostgresStreamingChangeEventSource(
                configuration,
                snapshotter,
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext,
                replicationConnection);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                              PostgresOffsetContext offsetContext,
                                                                                                                              SnapshotProgressListener snapshotProgressListener,
                                                                                                                              DataChangeEventListener dataChangeEventListener) {
        final SignalBasedIncrementalSnapshotChangeEventSource<TableId> incrementalSnapshotChangeEventSource = new SignalBasedIncrementalSnapshotChangeEventSource<TableId>(
                configuration,
                jdbcConnection,
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener);
        return Optional.of(incrementalSnapshotChangeEventSource);
    }
}
