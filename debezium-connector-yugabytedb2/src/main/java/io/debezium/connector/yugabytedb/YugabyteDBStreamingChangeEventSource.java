/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;
import org.yb.client.*;

import io.debezium.connector.yugabytedb.connection.*;
import io.debezium.connector.yugabytedb.connection.ReplicationMessage.Operation;
import io.debezium.connector.yugabytedb.connection.pgproto.YbProtoReplicationMessage;
import io.debezium.connector.yugabytedb.spi.Snapshotter;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.DelayStrategy;
import io.debezium.util.ElapsedTimeStrategy;

/**
 *
 * @author Suranjan Kumar (skumar@yugabyte.com)
 */
public class YugabyteDBStreamingChangeEventSource implements
        StreamingChangeEventSource<YugabyteDBPartition, YugabyteDBOffsetContext> {

    private static final String KEEP_ALIVE_THREAD_NAME = "keep-alive";

    /**
     * Number of received events without sending anything to Kafka which will
     * trigger a "WAL backlog growing" warning.
     */
    private static final int GROWING_WAL_WARNING_LOG_INTERVAL = 10_000;

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBStreamingChangeEventSource.class);

    // PGOUTPUT decoder sends the messages with larger time gaps than other decoders
    // We thus try to read the message multiple times before we make poll pause
    private static final int THROTTLE_NO_MESSAGE_BEFORE_PAUSE = 5;

    private final YugabyteDBConnection connection;
    private final EventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final YugabyteDBSchema schema;
    private final YugabyteDBConnectorConfig connectorConfig;
    private final YugabyteDBTaskContext taskContext; // todo vk: remove if not used

    private final Snapshotter snapshotter;
    private final DelayStrategy pauseNoMessage;
    private final ElapsedTimeStrategy connectionProbeTimer; // todo vk: remove if not used

    /**
     * The minimum of (number of event received since the last event sent to Kafka,
     * number of event received since last WAL growing warning issued).
     */
    private long numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
    private OpId lastCompletelyProcessedLsn;

    private final AsyncYBClient asyncYBClient;
    private final YBClient syncClient;
    private YugabyteDBTypeRegistry yugabyteDBTypeRegistry;
    private final Map<String, OpId> checkPointMap; // todo vk: remove if not used

    public YugabyteDBStreamingChangeEventSource(YugabyteDBConnectorConfig connectorConfig, Snapshotter snapshotter,
                                                YugabyteDBConnection connection, EventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock,
                                                YugabyteDBSchema schema, YugabyteDBTaskContext taskContext, ReplicationConnection replicationConnection) {
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        pauseNoMessage = DelayStrategy.constant(taskContext.getConfig().getPollInterval().toMillis());
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
        checkPointMap = new ConcurrentHashMap<>();
        // this.replicationConnection = replicationConnection;
        this.connectionProbeTimer = ElapsedTimeStrategy.constant(Clock.system(), connectorConfig.statusUpdateInterval());

        String masterAddress = connectorConfig.masterAddresses();
        asyncYBClient = new AsyncYBClient.AsyncYBClientBuilder(masterAddress)
                .defaultAdminOperationTimeoutMs(connectorConfig.adminOperationTimeoutMs())
                .defaultOperationTimeoutMs(connectorConfig.operationTimeoutMs())
                .defaultSocketReadTimeoutMs(connectorConfig.socketReadTimeoutMs())
                .numTablets(connectorConfig.maxNumTablets())
                .sslCertFile(connectorConfig.sslRootCert())
                .sslClientCertFiles(connectorConfig.sslClientCert(), connectorConfig.sslClientKey())
                .build();

        syncClient = new YBClient(asyncYBClient);
        yugabyteDBTypeRegistry = taskContext.schema().getTypeRegistry();
    }

    @Override
    public void execute(ChangeEventSourceContext context, YugabyteDBPartition partition, YugabyteDBOffsetContext offsetContext) {
        // replication slot could exist at the time of starting Debezium so
        // we will stream from the position in the slot
        // instead of the last position in the database
        // Get all partitions
        // Get
        Set<YBPartition> partitions = new YugabyteDBPartition.Provider(connectorConfig).getPartitions();
        boolean hasStartLsnStoredInContext = offsetContext != null && !offsetContext.getTabletSourceInfo().isEmpty();

        if (!hasStartLsnStoredInContext) {
            LOGGER.info("No initial OpId found in the context.");
            if (snapshotter.shouldSnapshot()) {
                offsetContext = YugabyteDBOffsetContext.initialContextForSnapshot(connectorConfig, connection, clock, partitions);
            }
            else {
                offsetContext = YugabyteDBOffsetContext.initialContext(connectorConfig, connection, clock, partitions);
            }
        }
        /*
         * if (snapshotter.shouldSnapshot()) {
         * getSnapshotChanges();
         * }
         */

        try {
            final WalPositionLocator walPosition;

            // todo vk: remove commented/dead code
            if (hasStartLsnStoredInContext) {
                // start streaming from the last recorded position in the offset
                final OpId lsn = offsetContext.lastCompletelyProcessedLsn() != null ? offsetContext.lastCompletelyProcessedLsn() : offsetContext.lsn();
                LOGGER.info("Retrieved latest position from stored offset '{}'", lsn);
                walPosition = new WalPositionLocator(offsetContext.lastCommitLsn(), lsn);
                // replicationStream.compareAndSet(null, replicationConnection.startStreaming(lsn, walPosition));
            }
            else {
                LOGGER.info("No previous LSN found in Kafka, streaming from the latest checkpoint" +
                        " in YugabyteDB");
                walPosition = new WalPositionLocator();
                // create snpashot offset.

                // replicationStream.compareAndSet(null, replicationConnection.startStreaming(walPosition));
            }

            getChanges(context, partition, offsetContext, hasStartLsnStoredInContext);
        }
        catch (Throwable e) {
            errorHandler.setProducerThrowable(e);
        }
        finally {

            if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {

            }
            if (asyncYBClient != null) {
                try {
                    asyncYBClient.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (syncClient != null) {
                try {
                    syncClient.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private GetChangesResponse getChangeResponse(YugabyteDBOffsetContext offsetContext) throws Exception {
        return null;
    }

    private void getSnapshotChanges(ChangeEventSourceContext context,
                                    YugabyteDBPartition partitionn,
                                    YugabyteDBOffsetContext offsetContext,
                                    boolean previousOffsetPresent) {

    }

    private boolean isSnapshotCompleteForAllTablets(Map<String, Boolean> snapshotDoneForTablet) {
        for (Map.Entry<String, Boolean> entry : snapshotDoneForTablet.entrySet()) {
            if (entry.getValue() == Boolean.FALSE) {
                return false;
            }
        }

        // Returning true would mean that we have captured the snapshot for all the tablets and in case of initial_only snapshotter
        // the snapshot can be stopped now.
        return true;
    }

    private void getChanges(ChangeEventSourceContext context,
                            YugabyteDBPartition partitionn,
                            YugabyteDBOffsetContext offsetContext,
                            boolean previousOffsetPresent)
            throws Exception {
        LOGGER.debug("The offset is " + offsetContext.getOffset());

        LOGGER.info("Proceeding to process change records now");
        ListTablesResponse tablesResp = syncClient.getTablesList();

        // todo vk: we have never used/tested this before
        String tabletList = this.connectorConfig.getConfig().getString(YugabyteDBConnectorConfig.TABLET_LIST);
        List<Pair<String, String>> tabletPairList = null;
        try {
            tabletPairList = (List<Pair<String, String>>) ObjectUtil.deserializeObjectFromString(tabletList);
            LOGGER.debug("The tablet list is " + tabletPairList);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        Map<String, YBTable> tableIdToTable = new HashMap<>();
        String streamId = connectorConfig.streamId();

        LOGGER.info("Using DB stream ID: " + streamId);

        Set<String> tIds = tabletPairList.stream().map(pair -> pair.getLeft()).collect(Collectors.toSet());
        for (String tId : tIds) {
            LOGGER.debug("Table UUID: " + tIds);
            YBTable table = this.syncClient.openTableByUUID(tId);
            tableIdToTable.put(tId, table);
        }

        // todo: rename schemaStreamed to something else
        Map<String, Boolean> schemaStreamed = new HashMap<>();
        Map<String, Boolean> snapshotDoneForTablet = new HashMap<>();

        // Initialize all the tabletIds with true signifying we need schemas for all the tablets.
        // Also initialize the snapshot flags for each tablet.
        for (Pair<String, String> entry : tabletPairList) {
            schemaStreamed.put(entry.getValue(), Boolean.TRUE);
            snapshotDoneForTablet.put(entry.getValue(), Boolean.FALSE);
        }

        // todo vk: add javadoc for the function getTabletSourceInfo
        LOGGER.info("The init tabletSourceInfo is " + offsetContext.getTabletSourceInfo());

        while (context.isRunning() && (offsetContext.getStreamingStoppingLsn() == null ||
                (lastCompletelyProcessedLsn.compareTo(offsetContext.getStreamingStoppingLsn()) < 0))) {
            // The following will specify the connector polling interval at which
            // yb-client will ask the database for changes
            LOGGER.debug("Sleeping for {} milliseconds before polling further", connectorConfig.cdcPollIntervalms());
            Thread.sleep(connectorConfig.cdcPollIntervalms());

            for (Pair<String, String> entry : tabletPairList) {
                final String tabletId = entry.getValue();
                YBPartition part = new YBPartition(tabletId);

                // Ignore this tablet if the snapshot is already complete for it.
                if (!snapshotter.shouldStream() && snapshotDoneForTablet.get(tabletId)) {
                    continue;
                }

                YBTable table = tableIdToTable.get(entry.getKey());
                OpId cp = offsetContext.lsn(tabletId);

                // GetChangesResponse response = getChangeResponse(offsetContext);
                LOGGER.debug("Fetching changes for the tablet {} from OpId {} for table {}", tabletId, cp, table.getName());

                GetChangesResponse response = this.syncClient.getChangesCDCSDK(
                        table, streamId, tabletId,
                        cp.getTerm(), cp.getIndex(), cp.getKey(), cp.getWrite_id(), cp.getTime(), schemaStreamed.get(tabletId));

                for (CdcService.CDCSDKProtoRecordPB record : response.getResp().getCdcSdkProtoRecordsList()) {
                    CdcService.RowMessage m = record.getRowMessage();
                    YbProtoReplicationMessage message = new YbProtoReplicationMessage(
                            m, this.yugabyteDBTypeRegistry);

                    String pgSchemaNameInRecord = m.getPgschemaName();

                    final OpId lsn = new OpId(record.getCdcSdkOpId().getTerm(),
                            record.getCdcSdkOpId().getIndex(),
                            record.getCdcSdkOpId().getWriteIdKey().toByteArray(),
                            record.getCdcSdkOpId().getWriteId(),
                            response.getSnapshotTime()); // stream.lastReceivedLsn();

                    if (message.isLastEventForLsn()) {
                        lastCompletelyProcessedLsn = lsn;
                    }

                    try {
                        // Tx BEGIN/END event
                        if (message.isTransactionalMessage()) {
                            if (!connectorConfig.shouldProvideTransactionMetadata()) {
                                LOGGER.debug("Received a transactional record {}", record);
                                // Don't skip on BEGIN message as it would flush LSN for the whole transaction
                                // too early
                                if (message.getOperation() == Operation.BEGIN) {
                                    LOGGER.debug("LSN in case of BEGIN is " + lsn);
                                }
                                if (message.getOperation() == Operation.COMMIT) {
                                    LOGGER.debug("LSN in case of COMMIT is " + lsn);
                                    offsetContext.updateWalPosition(tabletId, lsn, lastCompletelyProcessedLsn, message.getCommitTime(),
                                            String.valueOf(message.getTransactionId()), null, null/* taskContext.getSlotXmin(connection) */);
                                    commitMessage(part, offsetContext, lsn);
                                }
                                continue;
                            }

                            if (message.getOperation() == Operation.BEGIN) {
                                LOGGER.debug("LSN in case of BEGIN is " + lsn);
                                dispatcher.dispatchTransactionStartedEvent(part,
                                        message.getTransactionId(), offsetContext);
                            }
                            else if (message.getOperation() == Operation.COMMIT) {
                                LOGGER.debug("LSN in case of COMMIT is " + lsn);
                                offsetContext.updateWalPosition(tabletId, lsn, lastCompletelyProcessedLsn, message.getCommitTime(),
                                        String.valueOf(message.getTransactionId()), null, null/* taskContext.getSlotXmin(connection) */);
                                commitMessage(part, offsetContext, lsn);
                                dispatcher.dispatchTransactionCommittedEvent(part, offsetContext);
                            }
                            maybeWarnAboutGrowingWalBacklog(true);
                        }
                        else if (message.isDDLMessage()) {
                            LOGGER.debug("Received a DDL record for the table {}: {}", message.getTable(), message.getSchema().toString());

                            // Set schema received for this tablet ID which means that if a DDL message is received for a tablet,
                            // we do not need its schema again.
                            schemaStreamed.put(tabletId, Boolean.FALSE);

                            TableId tableId = null;
                            if (message.getOperation() != Operation.NOOP) {
                                tableId = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaNameInRecord);
                                Objects.requireNonNull(tableId);
                            }
                            // Getting the table with the help of the schema.
                            Table t = schema.tableFor(tableId);
                            LOGGER.debug("The schema is already registered {}", t);
                            if (t == null) {
                                // If we fail to achieve the table, that means we have not specified correct schema information,
                                // now try to refresh the schema.
                                schema.refreshWithSchema(tableId, message.getSchema(), pgSchemaNameInRecord);
                            }
                        }
                        // DML event
                        else {
                            TableId tableId = null;
                            if (message.getOperation() != Operation.NOOP) {
                                tableId = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaNameInRecord);
                                Objects.requireNonNull(tableId);
                            }

                            LOGGER.debug("Received a DML record: {}", record);

                            offsetContext.updateWalPosition(tabletId, lsn, lastCompletelyProcessedLsn, message.getCommitTime(),
                                    String.valueOf(message.getTransactionId()), tableId, null/* taskContext.getSlotXmin(connection) */);

                            boolean dispatched = message.getOperation() != Operation.NOOP
                                    && dispatcher.dispatchDataChangeEvent(tableId, new YugabyteDBChangeRecordEmitter(part, offsetContext, clock, connectorConfig,
                                            schema, connection, tableId, message, pgSchemaNameInRecord));

                            LOGGER.debug("Record dispatch status: {}", dispatched);

                            maybeWarnAboutGrowingWalBacklog(dispatched);
                        }
                    }
                    catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }
                    catch (SQLException se) {
                        se.printStackTrace();
                    }
                }

                if (!snapshotter.shouldStream() && response.getResp().getCdcSdkCheckpoint().getWriteId() != -1) {
                    LOGGER.debug("Marking snapshot complete for tablet: " + tabletId);
                    snapshotDoneForTablet.put(tabletId, Boolean.TRUE);
                }

                // End the snapshot in case the snapshot is complete.
                if (isSnapshotCompleteForAllTablets(snapshotDoneForTablet) && !snapshotter.shouldStream()) {
                    LOGGER.debug("Snapshot completed for all the tablets, the connector will not stop streaming as per the configuration");
                    return;
                }

                OpId finalOpid = new OpId(
                        response.getTerm(),
                        response.getIndex(),
                        response.getKey(),
                        response.getWriteId(),
                        response.getSnapshotTime());
                offsetContext.getSourceInfo(tabletId).updateLastCommit(finalOpid);

                probeConnectionIfNeeded();

                if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
                    // During catch up streaming, the streaming phase needs to hold a transaction open so that
                    // the phase can stream event up to a specific lsn and the snapshot that occurs after the catch up
                    // streaming will not lose the current view of data. Since we need to hold the transaction open
                    // for the snapshot, this block must not commit during catch up streaming.
                    // CDCSDK Find out why this fails : connection.commit();
                }
            }
        }
    }

    // todo vk: this function is not being used
    private void searchWalPosition(ChangeEventSourceContext context, final ReplicationStream stream, final WalPositionLocator walPosition)
            throws SQLException, InterruptedException {
        AtomicReference<OpId> resumeLsn = new AtomicReference<>();
        int noMessageIterations = 0;

        LOGGER.debug("Searching for WAL resume position");
        while (context.isRunning() && resumeLsn.get() == null) {

            boolean receivedMessage = stream.readPending(message -> {
                final OpId lsn = stream.lastReceivedLsn();
                resumeLsn.set(walPosition.resumeFromLsn(lsn, message).orElse(null));
            });

            if (receivedMessage) {
                noMessageIterations = 0;
            }
            else {
                noMessageIterations++;
                if (noMessageIterations >= THROTTLE_NO_MESSAGE_BEFORE_PAUSE) {
                    noMessageIterations = 0;
                    pauseNoMessage.sleepWhen(true);
                }
            }

            probeConnectionIfNeeded();
        }
        LOGGER.info("WAL resume position '{}' discovered", resumeLsn.get());
    }

    private void probeConnectionIfNeeded() throws SQLException {
        // CDCSDK Find out why it fails.
        // if (connectionProbeTimer.hasElapsed()) {
        // connection.prepareQuery("SELECT 1");
        // connection.commit();
        // }
    }

    private void commitMessage(YBPartition partition, YugabyteDBOffsetContext offsetContext,
                               final OpId lsn)
            throws SQLException, InterruptedException {
        lastCompletelyProcessedLsn = lsn;
        offsetContext.updateCommitPosition(lsn, lastCompletelyProcessedLsn);
        maybeWarnAboutGrowingWalBacklog(false);
        dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
    }

    /**
     * If we receive change events but all of them get filtered out, we cannot
     * commit any new offset with Apache Kafka. This in turn means no LSN is ever
     * acknowledged with the replication slot, causing any ever growing WAL backlog.
     * <p>
     * This situation typically occurs if there are changes on the database server,
     * (e.g. in an excluded database), but none of them is in table.include.list.
     * To prevent this, heartbeats can be used, as they will allow us to commit
     * offsets also when not propagating any "real" change event.
     * <p>
     * The purpose of this method is to detect this situation and log a warning
     * every {@link #GROWING_WAL_WARNING_LOG_INTERVAL} filtered events.
     *
     * @param dispatched
     *            Whether an event was sent to the broker or not
     */
    private void maybeWarnAboutGrowingWalBacklog(boolean dispatched) {
        if (dispatched) {
            numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
        }
        else {
            numberOfEventsSinceLastEventSentOrWalGrowingWarning++;
        }

        if (numberOfEventsSinceLastEventSentOrWalGrowingWarning > GROWING_WAL_WARNING_LOG_INTERVAL && !dispatcher.heartbeatsEnabled()) {
            LOGGER.warn("Received {} events which were all filtered out, so no offset could be committed. "
                    + "This prevents the replication slot from acknowledging the processed WAL offsets, "
                    + "causing a growing backlog of non-removeable WAL segments on the database server. "
                    + "Consider to either adjust your filter configuration or enable heartbeat events "
                    + "(via the {} option) to avoid this situation.",
                    numberOfEventsSinceLastEventSentOrWalGrowingWarning, Heartbeat.HEARTBEAT_INTERVAL_PROPERTY_NAME);

            numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
        }
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        // try {
        LOGGER.debug("Commit offset: " + offset);
        ReplicationStream replicationStream = null; // this.replicationStream.get();
        final OpId commitLsn = null; // OpId.valueOf((String) offset.get(PostgresOffsetContext.LAST_COMMIT_LSN_KEY));
        final OpId changeLsn = OpId.valueOf((String) offset.get(YugabyteDBOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY));
        final OpId lsn = (commitLsn != null) ? commitLsn : changeLsn;

        if (replicationStream != null && lsn != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Flushing LSN to server: {}", lsn);
            }
        }
        else {
            LOGGER.debug("Streaming has already stopped, ignoring commit callback...");
        }
        // }
        /*
         * catch (SQLException e) {
         * throw new ConnectException(e);
         * }
         */
    }

    /**
     * Returns whether the current streaming phase is running a catch up streaming
     * phase that runs before a snapshot. This is useful for transaction
     * management.
     *
     * During pre-snapshot catch up streaming, we open the snapshot transaction
     * early and hold the transaction open throughout the pre snapshot catch up
     * streaming phase so that we know where to stop streaming and can start the
     * snapshot phase at a consistent location. This is opposed the regular streaming,
     * where we do not a lingering open transaction.
     *
     * @return true if the current streaming phase is performing catch up streaming
     */
    private boolean isInPreSnapshotCatchUpStreaming(YugabyteDBOffsetContext offsetContext) {
        return offsetContext.getStreamingStoppingLsn() != null;
    }

    @FunctionalInterface
    public static interface PgConnectionSupplier {
        BaseConnection get() throws SQLException;
    }
}
