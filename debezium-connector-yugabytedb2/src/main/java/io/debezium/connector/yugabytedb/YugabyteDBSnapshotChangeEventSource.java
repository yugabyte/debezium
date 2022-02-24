/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.connector.yugabytedb.spi.Snapshotter;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

public class YugabyteDBSnapshotChangeEventSource extends AbstractSnapshotChangeEventSource<YugabyteDBPartition, YugabyteDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBSnapshotChangeEventSource.class);
    private final YugabyteDBConnectorConfig connectorConfig;
    private final YugabyteDBSchema schema;
    private final SnapshotProgressListener snapshotProgressListener;
    private final YugabyteDBTaskContext taskContext;
    private final EventDispatcher<TableId> dispatcher;
    protected final Clock clock;
    private final Snapshotter snapshotter;

    public YugabyteDBSnapshotChangeEventSource(YugabyteDBConnectorConfig connectorConfig,
                                               YugabyteDBTaskContext taskContext,
                                               Snapshotter snapshotter,
                                               YugabyteDBSchema schema, EventDispatcher<TableId> dispatcher, Clock clock,
                                               SnapshotProgressListener snapshotProgressListener) {
        super(connectorConfig, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.schema = schema;
        this.taskContext = taskContext;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotter = snapshotter;
        // this.errorHandler = errorHandler;

        this.snapshotProgressListener = snapshotProgressListener;
    }

    private Stream<TableId> toTableIds(Set<TableId> tableIds, Pattern pattern) {
        return tableIds
                .stream()
                .filter(tid -> pattern.asPredicate().test(connectorConfig.getTableIdMapper().toString(tid)))
                .sorted();
    }

    private Set<TableId> sort(Set<TableId> capturedTables) throws Exception {
        String tableIncludeList = connectorConfig.tableIncludeList();
        if (tableIncludeList != null) {
            return Strings.listOfRegex(tableIncludeList, Pattern.CASE_INSENSITIVE)
                    .stream()
                    .flatMap(pattern -> toTableIds(capturedTables, pattern))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
        return capturedTables
                .stream()
                .sorted()
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private void determineCapturedTables(RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> ctx)
            throws Exception {
        Set<TableId> allTableIds = determineDataCollectionsToBeSnapshotted(getAllTableIds(ctx)).collect(Collectors.toSet());

        Set<TableId> capturedTables = new HashSet<>();
        Set<TableId> capturedSchemaTables = new HashSet<>();

        for (TableId tableId : allTableIds) {
            if (connectorConfig.getTableFilters().eligibleDataCollectionFilter().isIncluded(tableId)) {
                LOGGER.trace("Adding table {} to the list of capture schema tables", tableId);
                capturedSchemaTables.add(tableId);
            }
            if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                LOGGER.trace("Adding table {} to the list of captured tables", tableId);
                capturedTables.add(tableId);
            }
            else {
                LOGGER.trace("Ignoring table {} as it's not included in the filter configuration", tableId);
            }
        }

        ctx.capturedTables = sort(capturedTables);
        ctx.capturedSchemaTables = capturedSchemaTables
                .stream()
                .sorted()
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public SnapshotResult<YugabyteDBOffsetContext> doExecute(
                                                             ChangeEventSourceContext context, YugabyteDBOffsetContext previousOffset,
                                                             SnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> snapshotContext,
                                                             SnapshottingTask snapshottingTask)
            throws Exception {
        return SnapshotResult.completed(previousOffset);
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(YugabyteDBOffsetContext previousOffset) {
        boolean snapshotSchema = true;
        boolean snapshotData = true;

        snapshotData = snapshotter.shouldSnapshot();
        if (snapshotData) {
            LOGGER.info("According to the connector configuration data will be snapshotted");
        }
        else {
            LOGGER.info("According to the connector configuration no snapshot will be executed");
            snapshotSchema = false;
        }

        return new SnapshottingTask(snapshotSchema, snapshotData);
    }

    @Override
    protected SnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> prepare(YugabyteDBPartition partition)
            throws Exception {
        return new PostgresSnapshotContext(partition, connectorConfig.databaseName());
    }

    protected Set<TableId> getAllTableIds(RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> ctx)
            throws Exception {
        // return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[]{ "TABLE" });
        return new HashSet<>();
    }

    protected void releaseSchemaSnapshotLocks(RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> snapshotContext)
            throws SQLException {
    }

    protected void determineSnapshotOffset(RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> ctx,
                                           YugabyteDBOffsetContext previousOffset)
            throws Exception {
        YugabyteDBOffsetContext offset = ctx.offset;
        if (offset == null) {
            // if (previousOffset != null && !snapshotter.shouldStreamEventsStartingFromSnapshot()) {
            // // The connect framework, not the connector, manages triggering committing offset state so the
            // // replication stream may not have flushed the latest offset state during catch up streaming.
            // // The previousOffset variable is shared between the catch up streaming and snapshot phases and
            // // has the latest known offset state.
            // offset = PostgresOffsetContext.initialContext(connectorConfig, jdbcConnection, getClock(),
            // previousOffset.lastCommitLsn(), previousOffset.lastCompletelyProcessedLsn());
            // }
            // else {
            // offset = PostgresOffsetContext.initialContext(connectorConfig, jdbcConnection, getClock());
            // }
            ctx.offset = offset;
        }

        updateOffsetForSnapshot(offset);
    }

    private void updateOffsetForSnapshot(YugabyteDBOffsetContext offset) throws SQLException {
        final OpId xlogStart = getTransactionStartLsn();
        // final long txId = jdbcConnection.currentTransactionId().longValue();
        // LOGGER.info("Read xlogStart at '{}' from transaction '{}'", xlogStart, txId);
        //
        // // use the old xmin, as we don't want to update it if in xmin recovery
        // offset.updateWalPosition(xlogStart, offset.lastCompletelyProcessedLsn(), clock.currentTime(), String.valueOf(txId), null, offset.xmin());
    }

    protected void updateOffsetForPreSnapshotCatchUpStreaming(YugabyteDBOffsetContext offset) throws SQLException {
        updateOffsetForSnapshot(offset);
        offset.setStreamingStoppingLsn(null/* OpId.valueOf(jdbcConnection.currentXLogLocation()) */);
    }

    // TOOD:CDCSDK get the offset from YB for snapshot.
    private OpId getTransactionStartLsn() throws SQLException {
        // if (slotCreatedInfo != null) {
        // // When performing an exported snapshot based on a newly created replication slot, the txLogStart position
        // // should be based on the replication slot snapshot transaction point. This is crucial so that if any
        // // SQL operations occur mid-snapshot that they'll be properly captured when streaming begins; otherwise
        // // they'll be lost.
        // return slotCreatedInfo.startLsn();
        // }
        // else if (!snapshotter.shouldStreamEventsStartingFromSnapshot() && startingSlotInfo != null) {
        // // Allow streaming to resume from where streaming stopped last rather than where the current snapshot starts.
        // SlotState currentSlotState = jdbcConnection.getReplicationSlotState(null/* connectorConfig.slotName() */,
        // connectorConfig.plugin().getPostgresPluginName());
        // return currentSlotState.slotLastFlushedLsn();
        // }

        return null;// OpId.valueOf(jdbcConnection.currentXLogLocation());
    }

    @Override
    protected void complete(SnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> snapshotContext) {
        snapshotter.snapshotCompleted();
    }

    /**
     * Generate a valid Postgres query string for the specified table and columns
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    protected Optional<String> getSnapshotSelect(
                                                 RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {
        return snapshotter.buildSnapshotQuery(tableId, columns);
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class PostgresSnapshotContext extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> {

        public PostgresSnapshotContext(YugabyteDBPartition partition, String catalogName) throws SQLException {
            super(partition, catalogName);
        }
    }
}
