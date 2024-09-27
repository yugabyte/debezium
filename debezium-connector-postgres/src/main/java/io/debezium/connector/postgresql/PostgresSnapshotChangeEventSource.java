/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.PostgresOffsetContext.Loader;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.snapshot.AlwaysSnapshotter;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;

public class PostgresSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSnapshotChangeEventSource.class);

    private final PostgresConnectorConfig connectorConfig;
    private final PostgresConnection jdbcConnection;
    private final PostgresSchema schema;
    private final Snapshotter snapshotter;
    private final Snapshotter blockingSnapshotter;
    private final SlotCreationResult slotCreatedInfo;
    private final SlotState startingSlotInfo;

    public PostgresSnapshotChangeEventSource(PostgresConnectorConfig connectorConfig, Snapshotter snapshotter,
                                             MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory, PostgresSchema schema,
                                             EventDispatcher<PostgresPartition, TableId> dispatcher, Clock clock,
                                             SnapshotProgressListener<PostgresPartition> snapshotProgressListener, SlotCreationResult slotCreatedInfo,
                                             SlotState startingSlotInfo, NotificationService<PostgresPartition, PostgresOffsetContext> notificationService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = connectionFactory.mainConnection();
        this.schema = schema;
        this.snapshotter = snapshotter;
        this.slotCreatedInfo = slotCreatedInfo;
        this.startingSlotInfo = startingSlotInfo;
        this.blockingSnapshotter = new AlwaysSnapshotter();
    }

    @Override
    public SnapshottingTask getSnapshottingTask(PostgresPartition partition, PostgresOffsetContext previousOffset) {

        boolean snapshotSchema = true;

        List<String> dataCollectionsToBeSnapshotted = connectorConfig.getDataCollectionsToBeSnapshotted();
        Map<String, String> snapshotSelectOverridesByTable = connectorConfig.getSnapshotSelectOverridesByTable().entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().identifier(), Map.Entry::getValue));

        boolean snapshotData = snapshotter.shouldSnapshot();
        if (snapshotData) {
            LOGGER.info("According to the connector configuration data will be snapshotted");
        }
        else {
            LOGGER.info("According to the connector configuration no snapshot will be executed");
            snapshotSchema = false;
        }

        return new SnapshottingTask(snapshotSchema, snapshotData, dataCollectionsToBeSnapshotted, snapshotSelectOverridesByTable, false);
    }

    @Override
    protected SnapshotContext<PostgresPartition, PostgresOffsetContext> prepare(PostgresPartition partition, boolean onDemand) {
        return new PostgresSnapshotContext(partition, connectorConfig.databaseName(), onDemand);
    }

    @Override
    protected ChangeRecordEmitter<PostgresPartition> getChangeRecordEmitter(
      PostgresPartition partition, PostgresOffsetContext offset, TableId tableId, Object[] row,
      Instant timestamp) {
        if (YugabyteDBServer.isEnabled() && connectorConfig.plugin().isYBOutput()) {
            offset.event(tableId, timestamp);

            return new YBSnapshotChangeRecordEmitter<>(partition, offset, row, getClock(),
              connectorConfig);
        } else {
            return super.getChangeRecordEmitter(partition, offset, tableId, row, timestamp);
        }
    }

    @Override
    protected void connectionCreated(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext)
            throws Exception {
        if (YugabyteDBServer.isEnabled()) {
            // In case of YB, the consistent snapshot is performed as follows -
            // 1) If connector created the slot, then the snapshotName returned as part of the CREATE_REPLICATION_SLOT
            //    command will have the hybrid time as of which the snapshot query is to be run
            // 2) If slot already exists, then the snapshot query will be run as of the hybrid time corresponding to the
            //    restart_lsn. This information is available in the pg_replication_slots view
            // In either case, the setSnapshotTransactionIsolationLevel function needs to be called so that the preparatory
            // commands can be run on the snapshot connection so that the snapshot query can be run as of the appropriate
            // hybrid time
            setSnapshotTransactionIsolationLevel(snapshotContext.onDemand);
        }
        else if (snapshotter.shouldStreamEventsStartingFromSnapshot() && startingSlotInfo == null) {
            // If using catch up streaming, the connector opens the transaction that the snapshot will eventually use
            // before the catch up streaming starts. By looking at the current wal location, the transaction can determine
            // where the catch up streaming should stop. The transaction is held open throughout the catch up
            // streaming phase so that the snapshot is performed from a consistent view of the data. Since the isolation
            // level on the transaction used in catch up streaming has already set the isolation level and executed
            // statements, the transaction does not need to get set the level again here.
            setSnapshotTransactionIsolationLevel(snapshotContext.onDemand);
        }
        schema.refresh(jdbcConnection, false);
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> ctx)
            throws Exception {
        return jdbcConnection.getAllTableIds(ctx.catalogName);
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext)
            throws SQLException {
        final Duration lockTimeout = connectorConfig.snapshotLockTimeout();
        final Optional<String> lockStatement = snapshotter.snapshotTableLockingStatement(lockTimeout, snapshotContext.capturedTables);

        if (lockStatement.isPresent()) {
            LOGGER.info("Waiting a maximum of '{}' seconds for each table lock", lockTimeout.getSeconds());
            jdbcConnection.executeWithoutCommitting(lockStatement.get());
            // now that we have the locks, refresh the schema
            schema.refresh(jdbcConnection, false);
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext)
            throws SQLException {
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> ctx, PostgresOffsetContext previousOffset)
            throws Exception {
        PostgresOffsetContext offset = ctx.offset;
        if (offset == null) {
            if (previousOffset != null && !snapshotter.shouldStreamEventsStartingFromSnapshot()) {
                // The connect framework, not the connector, manages triggering committing offset state so the
                // replication stream may not have flushed the latest offset state during catch up streaming.
                // The previousOffset variable is shared between the catch up streaming and snapshot phases and
                // has the latest known offset state.
                offset = PostgresOffsetContext.initialContext(connectorConfig, jdbcConnection, getClock(),
                        previousOffset.lastCommitLsn(), previousOffset.lastCompletelyProcessedLsn());
            }
            else {
                offset = PostgresOffsetContext.initialContext(connectorConfig, jdbcConnection, getClock());
            }
            ctx.offset = offset;
        }

        updateOffsetForSnapshot(offset);
    }

    private void updateOffsetForSnapshot(PostgresOffsetContext offset) throws SQLException {
        final Lsn xlogStart = getTransactionStartLsn();
        final Long txId = jdbcConnection.currentTransactionId();
        LOGGER.info("Read xlogStart at '{}' from transaction '{}'", xlogStart, txId);

        // use the old xmin, as we don't want to update it if in xmin recovery
        offset.updateWalPosition(xlogStart, offset.lastCompletelyProcessedLsn(), clock.currentTime(), txId, offset.xmin(), null, null);
    }

    protected void updateOffsetForPreSnapshotCatchUpStreaming(PostgresOffsetContext offset) throws SQLException {
        updateOffsetForSnapshot(offset);
        offset.setStreamingStoppingLsn(Lsn.valueOf(jdbcConnection.currentXLogLocation()));
    }

    private Lsn getTransactionStartLsn() throws SQLException {
        if (slotCreatedInfo != null) {
            // When performing an exported snapshot based on a newly created replication slot, the txLogStart position
            // should be based on the replication slot snapshot transaction point. This is crucial so that if any
            // SQL operations occur mid-snapshot that they'll be properly captured when streaming begins; otherwise
            // they'll be lost.
            return slotCreatedInfo.startLsn();
        }
        else if (YugabyteDBServer.isEnabled()) {
            // For YB, there are only 2 cases -
            // 1) Connector creates the slot - in this case (slotCreatedInfo != null) will hold
            // 2) Slot already exists - in this case, the streaming should start from the confirmed_flush_lsn
            SlotState currentSlotState = jdbcConnection.getReplicationSlotState(connectorConfig.slotName(),
                    connectorConfig.plugin().getPostgresPluginName());
            return currentSlotState.slotLastFlushedLsn();
        }
        else if (!snapshotter.shouldStreamEventsStartingFromSnapshot() && startingSlotInfo != null) {
            // Allow streaming to resume from where streaming stopped last rather than where the current snapshot starts.
            SlotState currentSlotState = jdbcConnection.getReplicationSlotState(connectorConfig.slotName(),
                    connectorConfig.plugin().getPostgresPluginName());
            return currentSlotState.slotLastFlushedLsn();
        }

        return Lsn.valueOf(jdbcConnection.currentXLogLocation());
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
                                      PostgresOffsetContext offsetContext, SnapshottingTask snapshottingTask)
            throws SQLException, InterruptedException {
        Set<String> schemas = snapshotContext.capturedTables.stream()
                .map(TableId::schema)
                .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }

            LOGGER.info("Reading structure of schema '{}' of catalog '{}'", schema, snapshotContext.catalogName);

            Tables.TableFilter tableFilter = snapshottingTask.isOnDemand() ? Tables.TableFilter.fromPredicate(snapshotContext.capturedTables::contains)
                    : connectorConfig.getTableFilters().dataCollectionFilter();

            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    snapshotContext.catalogName,
                    schema,
                    tableFilter,
                    null,
                    false);
        }
        schema.refresh(jdbcConnection, false);
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
                                                    Table table)
            throws SQLException {
        return SchemaChangeEvent.ofSnapshotCreate(snapshotContext.partition, snapshotContext.offset, snapshotContext.catalogName, table);
    }

    @Override
    protected void completed(SnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext) {
        snapshotter.snapshotCompleted();
    }

    @Override
    protected void aborted(SnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext) {
        snapshotter.snapshotAborted();
    }

    /**
     * Generate a valid Postgres query string for the specified table and columns
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {
        if (snapshotContext.onDemand) {
            return blockingSnapshotter.buildSnapshotQuery(tableId, columns);
        }

        return snapshotter.buildSnapshotQuery(tableId, columns);
    }

    @Override
    protected void doCreateDataEventsForTable(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
                                              PostgresOffsetContext offset, EventDispatcher.SnapshotReceiver<PostgresPartition> snapshotReceiver, Table table,
                                              boolean firstTable, boolean lastTable, int tableOrder, int tableCount, String selectStatement, OptionalLong rowCount,
                                              JdbcConnection jdbcConnection) throws InterruptedException {
        if (!sourceContext.isRunning()) {
            throw new InterruptedException("Interrupted while snapshotting table " + table.id());
        }

        long exportStart = clock.currentTimeInMillis();
        LOGGER.info("Exporting data from table '{}' ({} of {} tables)", table.id(), tableOrder, tableCount);

        Instant sourceTableSnapshotTimestamp = getSnapshotSourceTimestamp(jdbcConnection, offset, table.id());

        List<String> columns = getPreparedColumnNames(snapshotContext.partition, schema.tableFor(table.id()));

        List<String> queries = getParallelSnapshotQueries(table.id(), columns);

        for (int i = 0; i < queries.size(); ++i) {
            LOGGER.info("Executing snapshot query: {}", queries.get(i));
            try (Statement statement = readTableStatement(jdbcConnection, rowCount);
                    ResultSet rs = resultSetForDataEvents(queries.get(i), statement)) {

                ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
                long rows = 0;
                Threads.Timer logTimer = getTableScanLogTimer();
                boolean hasNext = rs.next();

                if (hasNext) {
                    while (hasNext) {
                        if (!sourceContext.isRunning()) {
                            throw new InterruptedException("Interrupted while snapshotting table " + table.id());
                        }

                        rows++;
                        final Object[] row = jdbcConnection.rowToArray(table, rs, columnArray);

                        if (logTimer.expired()) {
                            long stop = clock.currentTimeInMillis();
                            if (rowCount.isPresent()) {
                                LOGGER.info("\t Exported {} of {} records for table '{}' after {}", rows, rowCount.getAsLong(),
                                        table.id(), Strings.duration(stop - exportStart));
                            }
                            else {
                                LOGGER.info("\t Exported {} records for table '{}' after {}", rows, table.id(),
                                        Strings.duration(stop - exportStart));
                            }
                            //                        snapshotProgressListener.rowsScanned(snapshotContext.partition, table.id(), rows);
                            logTimer = getTableScanLogTimer();
                        }

                        hasNext = rs.next();
                        setSnapshotMarker(offset, firstTable, lastTable, rows == 1, !hasNext);

                        dispatcher.dispatchSnapshotEvent(snapshotContext.partition, table.id(),
                                getChangeRecordEmitter(snapshotContext.partition, offset, table.id(), row, sourceTableSnapshotTimestamp), snapshotReceiver);
                    }
                }
                else {
                                    setSnapshotMarker(offset, firstTable, lastTable, false, true);
                }

                LOGGER.info("\t Finished exporting {} records for table '{}' ({} of {} tables); total duration '{}'",
                        rows, table.id(), tableOrder, tableCount, Strings.duration(clock.currentTimeInMillis() - exportStart));
                            snapshotProgressListener.dataCollectionSnapshotCompleted(snapshotContext.partition, table.id(), rows);
                notificationService.initialSnapshotNotificationService().notifyCompletedTableSuccessfully(snapshotContext.partition,
                        snapshotContext.offset, table.id().identifier(), rows, snapshotContext.capturedTables);
            }
            catch (SQLException e) {
                notificationService.initialSnapshotNotificationService().notifyCompletedTableWithError(snapshotContext.partition,
                        snapshotContext.offset,
                        table.id().identifier());
                throw new ConnectException("Snapshotting of table " + table.id() + " with query " + queries.get(i) + " failed", e);
            }
        }
    }

    protected List<String> getParallelSnapshotQueries(TableId tableId, List<String> columns) {
        int numTasks = 5;
        final long rangeSize = (64 * 1024) / numTasks;

        // todo: yb_hash_code will take column names as parameter which should be configurable
        final String stmtFormat =
            columns.stream().collect(
                Collectors.joining(", ", "SELECT ", " FROM " + tableId.toDoubleQuotedString() + " WHERE yb_hash_code(id) >= %d AND yb_hash_code(id) <= %d"));

        List<String> queries = new ArrayList<>();
        for (int i = 0; i < numTasks; ++i) {
            long lowerBound = i * rangeSize;
            long upperBound = lowerBound + rangeSize - 1;

            queries.add(String.format(stmtFormat, lowerBound, upperBound));
        }

        return queries;
    }

    protected void setSnapshotTransactionIsolationLevel(boolean isOnDemand) throws SQLException {
        if (!YugabyteDBServer.isEnabled() || connectorConfig.isYbConsistentSnapshotEnabled()) {
            LOGGER.info("Setting isolation level");
            String transactionStatement = snapshotter.snapshotTransactionIsolationLevelStatement(slotCreatedInfo, isOnDemand);
            LOGGER.info("Opening transaction with statement {}", transactionStatement);
            jdbcConnection.executeWithoutCommitting(transactionStatement);
        } else {
            LOGGER.info("Skipping setting snapshot time, snapshot data will not be consistent");
        }
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class PostgresSnapshotContext extends RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> {

        PostgresSnapshotContext(PostgresPartition partition, String catalogName, boolean onDemand) {
            super(partition, catalogName, onDemand);
        }
    }

    @Override
    protected PostgresOffsetContext copyOffset(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext) {
        return new Loader(connectorConfig).load(snapshotContext.offset.getOffset());
    }
}
