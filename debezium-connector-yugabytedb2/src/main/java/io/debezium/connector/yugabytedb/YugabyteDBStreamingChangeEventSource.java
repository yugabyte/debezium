/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import static io.debezium.connector.yugabytedb.YugabyteDBSchema.PUBLIC_SCHEMA_NAME;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;
import org.yb.client.*;

import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.connector.yugabytedb.connection.ReplicationConnection;
import io.debezium.connector.yugabytedb.connection.ReplicationMessage.Operation;
import io.debezium.connector.yugabytedb.connection.ReplicationStream;
import io.debezium.connector.yugabytedb.connection.WalPositionLocator;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.connector.yugabytedb.connection.pgproto.PgProtoReplicationMessage;
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
import org.yb.master.MasterDdlOuterClass;

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
    private final YugabyteDBTaskContext taskContext;

    // private final ReplicationConnection replicationConnection;
    // private final AtomicReference<ReplicationStream> replicationStream = new AtomicReference<>();

    private final Snapshotter snapshotter;
    private final DelayStrategy pauseNoMessage;
    private final ElapsedTimeStrategy connectionProbeTimer;

    /**
     * The minimum of (number of event received since the last event sent to Kafka,
     * number of event received since last WAL growing warning issued).
     */
    private long numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
    private OpId lastCompletelyProcessedLsn;

    private final AsyncYBClient asyncYBClient;
    private final YBClient syncClient;
    private YugabyteDBTypeRegistry yugabyteDBTypeRegistry;
    private final Map<String, OpId> checkPointMap;

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

//        String masterAddress = connectorConfig.masterHost() + ":" + connectorConfig.masterPort();
        String masterAddress = connectorConfig.masterAddresses();
        asyncYBClient = new AsyncYBClient.AsyncYBClientBuilder(masterAddress)
                .defaultAdminOperationTimeoutMs(connectorConfig.adminOperationTimeoutMs())
                .defaultOperationTimeoutMs(connectorConfig.operationTimeoutMs())
                .defaultSocketReadTimeoutMs(connectorConfig.socketReadTimeoutMs())
                .numTablets(this.connectorConfig.maxNumTablets())
                .build();

        syncClient = new YBClient(asyncYBClient);
        yugabyteDBTypeRegistry = taskContext.schema().getTypeRegistry();
    }

    @Override
    public void execute(ChangeEventSourceContext context, YugabyteDBPartition partition, YugabyteDBOffsetContext offsetContext) {
        if (!snapshotter.shouldStream()) {
            LOGGER.info("Streaming is not enabled in correct configuration");
            return;
        }

        // replication slot could exist at the time of starting Debezium so
        // we will stream from the position in the slot
        // instead of the last position in the database
        boolean hasStartLsnStoredInContext = offsetContext != null;

        if (!hasStartLsnStoredInContext) {
            offsetContext = YugabyteDBOffsetContext.initialContext(connectorConfig, connection, clock);
        }

        try {
            final WalPositionLocator walPosition;

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
                // replicationStream.compareAndSet(null, replicationConnection.startStreaming(walPosition));
            }
            // for large dbs, the refresh of schema can take too much time
            // such that the connection times out. We must enable keep
            // alive to ensure that it doesn't time out
            // ReplicationStream stream = this.replicationStream.get();
            // stream.startKeepAlive(Threads.newSingleThreadExecutor(PostgresConnector.class, connectorConfig.getLogicalName(), KEEP_ALIVE_THREAD_NAME));

            // refresh the schema so we have a latest view of the DB tables
            // taskContext.refreshSchema(connection, true);

            // If we need to do a pre-snapshot streaming catch up, we should allow the snapshot transaction to persist
            // but normally we want to start streaming without any open transactions.
            // if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
            // connection.commit();
            // }

            // this.lastCompletelyProcessedLsn = replicationStream.get().startLsn();

            // if (walPosition.searchingEnabled()) {
            // searchWalPosition(context, stream, walPosition);
            // try {
            // if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
            // connection.commit();
            // }
            // }
            // catch (Exception e) {
            // LOGGER.info("Commit failed while preparing for reconnect", e);
            // }
            // walPosition.enableFiltering();
            // stream.stopKeepAlive();
            // replicationConnection.reconnect();
            // replicationStream.set(replicationConnection.startStreaming(walPosition.getLastEventStoredLsn(), walPosition));
            // stream = this.replicationStream.get();
            // stream.startKeepAlive(Threads.newSingleThreadExecutor(PostgresConnector.class, connectorConfig.getLogicalName(), KEEP_ALIVE_THREAD_NAME));
            // }
            // processMessages(context, partition, offsetContext, stream);
            getChanges2(context, partition, offsetContext);
        }
        catch (Throwable e) {
            errorHandler.setProducerThrowable(e);
        }
        finally {

            if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
                // Need to CDCSDK see what can be done.
                // try {
                // connection.commit();
                // }
                // catch (SQLException throwables) {
                // throwables.printStackTrace();
                // }
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
            // if (replicationConnection != null) {
            // LOGGER.debug("stopping streaming...");
            // // stop the keep alive thread, this also shuts down the
            // // executor pool
            // ReplicationStream stream = replicationStream.get();
            // if (stream != null) {
            // stream.stopKeepAlive();
            // }
            // // TODO author=Horia Chiorean date=08/11/2016 description=Ideally we'd close the stream, but it's not reliable atm (see javadoc)
            // // replicationStream.close();
            // // close the connection - this should also disconnect the current stream even if it's blocking
            // try {
            // if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
            // connection.commit();
            // }
            // replicationConnection.close();
            // }
            // catch (Exception e) {
            // LOGGER.debug("Exception while closing the connection", e);
            // }
            // replicationStream.set(null);
            // }
        }
    }

    private GetChangesResponse getChangeResponse(YugabyteDBOffsetContext offsetContext) throws Exception {

        return null;
    }

    private void getChanges2(ChangeEventSourceContext context,
                             YugabyteDBPartition partition,
                             YugabyteDBOffsetContext offsetContext)
            throws Exception {
        LOGGER.info("SKSK The offset is " + offsetContext.getOffset());

        LOGGER.info("Processing messages");
        ListTablesResponse tablesResp = syncClient.getTablesList();
        Set<String> tIds = new HashSet<>();

        for (MasterDdlOuterClass.ListTablesResponsePB.TableInfo tableInfo : tablesResp.getTableInfoList()) {
            LOGGER.info("SKSK The table name is " + tableInfo.getName());
            String fqlTableName = tableInfo.getNamespace().getName() + "." + "" + PUBLIC_SCHEMA_NAME
                    + "." + tableInfo.getName();
            TableId tableId = YugabyteDBSchema.parse(fqlTableName);
            if (this.connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                tIds.add(tableInfo.getId().toStringUtf8());
            }
        }

        Map<String, YBTable> tableIdToTable = new HashMap<>();
        String streamId = this.connectorConfig.streamId();
        LOGGER.info(String.format("Using stream with id %s", streamId));

        List<Map<String, List<String>>> tableIdsToTabletIdsMapList = new ArrayList<>(1);
        int concurrency = 1;
        boolean createStream = true;
        // if the user has specified a stream ID in the configuration, do not create a stream and use
        // the specified one
        if(streamId != null && streamId.isEmpty() == false) {
            LOGGER.info("VKVK: Using a user specified stream ID: " + streamId);
            createStream = false;
        }
        for (String tId : tIds) {
            LOGGER.info("SKSK the table uuid is " + tIds);
            YBTable table = this.syncClient.openTableByUUID(tId);
            tableIdToTable.put(tId, table);
            if (createStream) {
                streamId = this.syncClient.createCDCStream(table, "yugabyte",
                        "PROTO",
                        "IMPLICIT").getStreamId();
                createStream = false;
            }
            List<LocatedTablet> tabletLocations = table.getTabletsLocations(30000);

            for (int i = 0; i < concurrency; i++) {
                tableIdsToTabletIdsMapList.add(new HashMap<>());
            }
            int i = 0;
            for (LocatedTablet tablet : tabletLocations) {
                i++;
                String tabletId = new String(tablet.getTabletId());
                LOGGER.info(String.format("Polling for new tablet %s", tabletId));
                tableIdsToTabletIdsMapList.get(i % concurrency).putIfAbsent(tId, new ArrayList<>());
                tableIdsToTabletIdsMapList.get(i % concurrency).get(tId).add(tabletId);
            }
            LOGGER.info("SKSK The final map is " + tableIdsToTabletIdsMapList);
        }
        Map<String, List<String>> tableIdsToTabletIds = tableIdsToTabletIdsMapList.get(0);
        List<AbstractMap.SimpleImmutableEntry<String, String>> listTabletIdTableIdPair;
        listTabletIdTableIdPair = tableIdsToTabletIds.entrySet().stream()
                .flatMap(e -> e.getValue().stream()
                        .map(v -> new AbstractMap.SimpleImmutableEntry<>(v, e.getKey())))
                .collect(Collectors.toList());
        LOGGER.debug("The listTabletIdTableId is " + listTabletIdTableIdPair);

        int noMessageIterations = 0;
        for (AbstractMap.SimpleImmutableEntry<String, String> entry : listTabletIdTableIdPair) {
            final String tabletId = entry.getKey();
            offsetContext.initSourceInfo(tabletId, this.connectorConfig);
        }
        LOGGER.info("The init tabletSourceInfo is " + offsetContext.getTabletSourceInfo());

        while (context.isRunning() && (offsetContext.getStreamingStoppingLsn() == null ||
                (lastCompletelyProcessedLsn.compareTo(offsetContext.getStreamingStoppingLsn()) < 0))) {

            for (AbstractMap.SimpleImmutableEntry<String, String> entry : listTabletIdTableIdPair) {
                final String tabletId = entry.getKey();
                // Thread.sleep(200);
                YBTable table = tableIdToTable.get(entry.getValue());
                OpId cp = offsetContext.lsn(tabletId);
                // GetChangesResponse response = getChangeResponse(offsetContext);
                LOGGER.info("Going to fetch for tablet " + tabletId + " from OpId " + cp + " " +
                        "table " + table.getName());

                GetChangesResponse response = this.syncClient.getChangesCDCSDK(
                        table, streamId, tabletId,
                        cp.getTerm(), cp.getIndex(), cp.getKey(), cp.getWrite_id(), cp.getTime());

                boolean receivedMessage = response.getResp().getCdcSdkRecordsCount() != 0;

                for (CdcService.CDCSDKProtoRecordPB record : response
                        .getResp()
                        .getCdcSdkProtoRecordsList()) {
                    CdcService.RowMessage m = record.getRowMessage();
                    PgProtoReplicationMessage message = new PgProtoReplicationMessage(
                            m, this.yugabyteDBTypeRegistry);

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
                                LOGGER.info("Received transactional message {}", record);
                                // Don't skip on BEGIN message as it would flush LSN for the whole transaction
                                // too early
                                if (message.getOperation() == Operation.BEGIN) {
                                    LOGGER.info("LSN in case of BEGIN is " + lsn);
                                }
                                if (message.getOperation() == Operation.COMMIT) {
                                    LOGGER.info("LSN in case of COMMIT is " + lsn);
                                    offsetContext.updateWalPosition(tabletId, lsn,
                                            lastCompletelyProcessedLsn,
                                            message.getCommitTime(), String
                                                    .valueOf(message.getTransactionId()),
                                            null,
                                            null/* taskContext.getSlotXmin(connection) */);
                                    commitMessage(partition, offsetContext, lsn);
                                }
                                continue;
                            }

                            if (message.getOperation() == Operation.BEGIN) {
                                LOGGER.info("LSN in case of BEGIN is " + lsn);
                                dispatcher.dispatchTransactionStartedEvent(partition,
                                        message.getTransactionId(), offsetContext);
                            }
                            else if (message.getOperation() == Operation.COMMIT) {
                                LOGGER.info("LSN in case of COMMIT is " + lsn);
                                offsetContext.updateWalPosition(tabletId, lsn,
                                        lastCompletelyProcessedLsn,
                                        message.getCommitTime(),
                                        String.valueOf(message.getTransactionId()),
                                        null,
                                        null/* taskContext.getSlotXmin(connection) */);
                                commitMessage(partition, offsetContext, lsn);
                                dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext);
                            }
                            maybeWarnAboutGrowingWalBacklog(true);
                        }
                        else if (message.isDDLMessage()) {
                            LOGGER.info("Received DDL message {}", message.getSchema().toString()
                                    + " the table is " + message.getTable());
                            // TODO: Update the schema
                            // final String catalogName = "yugabyte";
                            TableId tableId = null;
                            if (message.getOperation() != Operation.NOOP) {
                                tableId = YugabyteDBSchema.parse(message.getTable());
                                Objects.requireNonNull(tableId);
                            }
                            Table t = schema.tableFor(tableId);
                            LOGGER.info("The schema is already registered {}", t);
                            if (t == null) {
                                schema.refresh(tableId, message.getSchema());
                            }
                        }
                        // DML event
                        else {
                            TableId tableId = null;
                            if (message.getOperation() != Operation.NOOP) {
                                tableId = YugabyteDBSchema.parse(message.getTable());
                                Objects.requireNonNull(tableId);
                            }
                            LOGGER.info("Received DML record {}", record);

                            offsetContext.updateWalPosition(tabletId, lsn, lastCompletelyProcessedLsn,
                                    message.getCommitTime(),
                                    String.valueOf(message.getTransactionId()), tableId,
                                    null/* taskContext.getSlotXmin(connection) */);

                            boolean dispatched = message.getOperation() != Operation.NOOP
                                    && dispatcher
                                            .dispatchDataChangeEvent(tableId,
                                                    new YugabyteDBChangeRecordEmitter(
                                                            partition,
                                                            offsetContext,
                                                            clock,
                                                            connectorConfig,
                                                            schema,
                                                            connection,
                                                            tableId,
                                                            message));

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

                probeConnectionIfNeeded();

                if (receivedMessage) {
                    noMessageIterations = 0;
                }
                else {
                    if (offsetContext.hasCompletelyProcessedPosition()) {
                        dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                    }
                    noMessageIterations++;
                    if (noMessageIterations >= THROTTLE_NO_MESSAGE_BEFORE_PAUSE) {
                        noMessageIterations = 0;
                        pauseNoMessage.sleepWhen(true);
                    }
                }
                if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
                    // During catch up streaming, the streaming phase needs to hold a transaction open so that
                    // the phase can stream event up to a specific lsn and the snapshot that occurs after the catch up
                    // streaming will not lose the current view of data. Since we need to hold the transaction open
                    // for the snapshot, this block must not commit during catch up streaming.
                    // CDCSDK Find out why this fails : connection.commit();
                    // todo vaibhav: the above fails because the connection's auto-commit has been set to false while
                    // initializing (check with Suranjan if this is what he was looking for)
                }
            }
        }
    }

    private void searchWalPosition(ChangeEventSourceContext context, final ReplicationStream stream, final WalPositionLocator walPosition)
            throws SQLException, InterruptedException {
        AtomicReference<OpId> resumeLsn = new AtomicReference<>();
        int noMessageIterations = 0;

        LOGGER.info("Searching for WAL resume position");
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

    private void commitMessage(YugabyteDBPartition partition, YugabyteDBOffsetContext offsetContext,
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
        LOGGER.info("SKSK the commitoffset is " + offset);
        ReplicationStream replicationStream = null;// this.replicationStream.get();
        final OpId commitLsn = null;// OpId.valueOf((String) offset.get(PostgresOffsetContext.LAST_COMMIT_LSN_KEY));
        final OpId changeLsn = OpId.valueOf((String) offset.get(YugabyteDBOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY));
        final OpId lsn = (commitLsn != null) ? commitLsn : changeLsn;

        if (replicationStream != null && lsn != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Flushing LSN to server: {}", lsn);
            }
            // tell the server the point up to which we've processed data, so it can be free to recycle WAL segments
            // CDCSDK yugabyte does it automatically.
            // but we may need an API
            // replicationStream.flushLsn(lsn);
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
