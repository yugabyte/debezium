/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.util.Optional;

import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
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

public class YugabyteDBChangeEventSourceFactory implements ChangeEventSourceFactory<YugabyteDBPartition, YugabyteDBOffsetContext> {

    private final YugabyteDBConnectorConfig configuration;
    private final YugabyteDBConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final YugabyteDBSchema schema;
    private final YugabyteDBTaskContext taskContext;
    private final Snapshotter snapshotter;

    public YugabyteDBChangeEventSourceFactory(YugabyteDBConnectorConfig configuration,
                                              Snapshotter snapshotter,
                                              YugabyteDBConnection jdbcConnection,
                                              ErrorHandler errorHandler,
                                              EventDispatcher<TableId> dispatcher,
                                              Clock clock, YugabyteDBSchema schema,
                                              YugabyteDBTaskContext taskContext) {
        this.configuration = configuration;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
    }

    @Override
    public SnapshotChangeEventSource<YugabyteDBPartition, YugabyteDBOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener snapshotProgressListener) {
        return new YugabyteDBSnapshotChangeEventSource(
                configuration,
                taskContext,
                snapshotter,
                schema,
                dispatcher,
                clock,
                snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource<YugabyteDBPartition, YugabyteDBOffsetContext> getStreamingChangeEventSource() {
        return new YugabyteDBStreamingChangeEventSource(
                configuration,
                snapshotter,
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
            YugabyteDBOffsetContext offsetContext,
            SnapshotProgressListener snapshotProgressListener,
            DataChangeEventListener dataChangeEventListener) {
        final SignalBasedIncrementalSnapshotChangeEventSource<TableId> incrementalSnapshotChangeEventSource = new SignalBasedIncrementalSnapshotChangeEventSource<>(
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
