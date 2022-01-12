/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.postgresql.core.Encoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.yugabytedb.connection.ReplicationConnection;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection.YugabyteDBValueConverterBuilder;
import io.debezium.connector.yugabytedb.spi.Snapshotter;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Kafka connect source task which uses YugabyteDB CDC API to process DB changes.
 *
 * @author Suranjan Kumar (skumar@yugabyte.com)
 */
public class YugabyteDBConnectorTask
        extends BaseSourceTask<YugabyteDBPartition, YugabyteDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBConnectorTask.class);
    private static final String CONTEXT_NAME = "yugabytedb-connector-task";

    private volatile YugabyteDBTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile YugabyteDBConnection jdbcConnection;
    private volatile YugabyteDBConnection heartbeatConnection;
    private volatile YugabyteDBSchema schema;

    @Override
    public ChangeEventSourceCoordinator<YugabyteDBPartition, YugabyteDBOffsetContext> start(Configuration config) {
        final YugabyteDBConnectorConfig connectorConfig = new YugabyteDBConnectorConfig(config);
        final TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(connectorConfig);
        final Snapshotter snapshotter = connectorConfig.getSnapshotter();
        final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

        LOGGER.info("The config is " + config);

        LOGGER.info("The tablet list is " + config.getString(YugabyteDBConnectorConfig.TABLET_LIST));
        if (snapshotter == null) {
            throw new ConnectException("Unable to load snapshotter, if using custom snapshot mode," +
                    " double check your settings");
        }

        final String databaseCharsetName = config.getString(YugabyteDBConnectorConfig.CHAR_SET);
        final Charset databaseCharset = Charset.forName(databaseCharsetName);

        String nameToTypeStr = config.getString(YugabyteDBConnectorConfig.NAME_TO_TYPE.toString());
        String oidToTypeStr = config.getString(YugabyteDBConnectorConfig.OID_TO_TYPE.toString());
        Encoding encoding = Encoding.defaultEncoding(); // UTF-8

        Map<String, YugabyteDBType> nameToType = null;
        Map<Integer, YugabyteDBType> oidToType = null;
        try {
            nameToType = (Map<String, YugabyteDBType>) ObjectUtil
                    .deserializeObjectFromString(nameToTypeStr);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            oidToType = (Map<Integer, YugabyteDBType>) ObjectUtil
                    .deserializeObjectFromString(oidToTypeStr);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        YugabyteDBTaskConnection taskConnection = new YugabyteDBTaskConnection(encoding);
        final YugabyteDBValueConverterBuilder valueConverterBuilder = (typeRegistry) -> YugabyteDBValueConverter.of(
                connectorConfig,
                databaseCharset,
                typeRegistry);

        // Global JDBC connection used both for snapshotting and streaming.
        // Must be able to resolve datatypes.
        // connection = new YugabyteDBConnection(connectorConfig.getJdbcConfig());
        jdbcConnection = new YugabyteDBConnection(connectorConfig.getJdbcConfig(),
                valueConverterBuilder);
        // try {
        // jdbcConnection.setAutoCommit(false);
        // }
        // catch (SQLException e) {
        // throw new DebeziumException(e);
        // }

        // CDCSDK We can just build the type registry on the co-ordinator and then send the
        // map of Postgres Type and Oid to the Task using Config
        final YugabyteDBTypeRegistry yugabyteDBTypeRegistry = new YugabyteDBTypeRegistry(taskConnection,
                nameToType, oidToType);
        // jdbcConnection.getTypeRegistry();

        schema = new YugabyteDBSchema(connectorConfig, yugabyteDBTypeRegistry, topicSelector,
                valueConverterBuilder.build(yugabyteDBTypeRegistry));
        this.taskContext = new YugabyteDBTaskContext(connectorConfig, schema, topicSelector);
        final Offsets<YugabyteDBPartition, YugabyteDBOffsetContext> previousOffsets = getPreviousOffsets(new YugabyteDBPartition.Provider(connectorConfig),
                new YugabyteDBOffsetContext.Loader(connectorConfig));
        final Clock clock = Clock.system();
        final YugabyteDBOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        LoggingContext.PreviousContext previousContext = taskContext
                .configureLoggingContext(CONTEXT_NAME);
        try {
            // Print out the server information
            // CDCSDK Get the table,
            /*
             * SlotState slotInfo = null;
             * try {
             * if (LOGGER.isInfoEnabled()) {
             * LOGGER.info(jdbcConnection.serverInfo().toString());
             * }
             * slotInfo = jdbcConnection.getReplicationSlotState(null
             *//* connectorConfig.slotName() *//*
                                                * , connectorConfig.plugin().getPostgresPluginName());
                                                * }
                                                * catch (SQLException e) {
                                                * LOGGER.warn("unable to load info of replication slot, Debezium will try to create the slot");
                                                * }
                                                * 
                                                * if (previousOffset == null) {
                                                * LOGGER.info("No previous offset found");
                                                * // if we have no initial offset, indicate that to Snapshotter by passing null
                                                * snapshotter.init(connectorConfig, null, slotInfo);
                                                * }
                                                * else {
                                                * LOGGER.info("Found previous offset {}", previousOffset);
                                                * snapshotter.init(connectorConfig, previousOffset.asOffsetState(), slotInfo);
                                                * }
                                                */

            ReplicationConnection replicationConnection = null;
            /*
             * SlotCreationResult slotCreatedInfo = null;
             * if (snapshotter.shouldStream()) {
             * final boolean doSnapshot = snapshotter.shouldSnapshot();
             * replicationConnection = createReplicationConnection(this.taskContext,
             * doSnapshot, connectorConfig.maxRetries(), connectorConfig.retryDelay());
             * 
             * // we need to create the slot before we start streaming if it doesn't exist
             * // otherwise we can't stream back changes happening while the snapshot is taking
             * place
             * if (slotInfo == null) {
             * try {
             * slotCreatedInfo = replicationConnection.createReplicationSlot().orElse(null);
             * }
             * catch (SQLException ex) {
             * String message = "Creation of replication slot failed";
             * if (ex.getMessage().contains("already exists")) {
             * message += "; when setting up multiple connectors for the same database host,
             * please make sure to use a distinct replication slot name for each.";
             * }
             * throw new DebeziumException(message, ex);
             * }
             * }
             * else {
             * slotCreatedInfo = null;
             * }
             * }
             */

            // try {
            // jdbcConnection.commit();
            // }
            // catch (SQLException e) {
            // throw new DebeziumException(e);
            // }

            queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(connectorConfig.getPollInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize())
                    .maxQueueSize(connectorConfig.getMaxQueueSize())
                    .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                    .build();

            ErrorHandler errorHandler = new PostgresErrorHandler(connectorConfig.getLogicalName(),
                    queue);

            final PostgresEventMetadataProvider metadataProvider = new PostgresEventMetadataProvider();

            Configuration configuration = connectorConfig.getConfig();
            Heartbeat heartbeat = Heartbeat.create(
                    configuration.getDuration(Heartbeat.HEARTBEAT_INTERVAL, ChronoUnit.MILLIS),
                    configuration.getString(DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY),
                    topicSelector.getHeartbeatTopic(),
                    connectorConfig.getLogicalName(), heartbeatConnection, exception -> {
                        String sqlErrorId = exception.getSQLState();
                        switch (sqlErrorId) {
                            case "57P01":
                                // Postgres error admin_shutdown, see
                                // https://www.postgresql.org/docs/12/errcodes-appendix.html
                                throw new DebeziumException("Could not execute heartbeat action" +
                                        " (Error: " + sqlErrorId + ")", exception);
                            case "57P03":
                                // Postgres error cannot_connect_now, see
                                // https://www.postgresql.org/docs/12/errcodes-appendix.html
                                throw new RetriableException("Could not execute heartbeat action" +
                                        " (Error: " + sqlErrorId + ")", exception);
                            default:
                                break;
                        }
                    });

            final EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
                    connectorConfig,
                    topicSelector,
                    schema,
                    queue,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    DataChangeEvent::new,
                    PostgresChangeRecordEmitter::updateSchema,
                    metadataProvider,
                    heartbeat,
                    schemaNameAdjuster,
                    jdbcConnection);

            ChangeEventSourceCoordinator<YugabyteDBPartition, YugabyteDBOffsetContext> coordinator = new PostgresChangeEventSourceCoordinator(
                    previousOffsets,
                    errorHandler,
                    YugabyteDBConnector.class,
                    connectorConfig,
                    new YugabyteDBChangeEventSourceFactory(
                            connectorConfig,
                            snapshotter,
                            jdbcConnection,
                            errorHandler,
                            dispatcher,
                            clock,
                            schema,
                            taskContext,
                            replicationConnection,
                            null/* slotCreatedInfo */,
                            null/* slotInfo */),
                    new DefaultChangeEventSourceMetricsFactory(),
                    dispatcher,
                    schema,
                    snapshotter,
                    null/* slotInfo */);

            coordinator.start(taskContext, this.queue, metadataProvider);

            return coordinator;
        }
        finally {
            previousContext.restore();
        }
    }

    public ReplicationConnection createReplicationConnection(YugabyteDBTaskContext taskContext,
                                                             boolean doSnapshot,
                                                             int maxRetries,
                                                             Duration retryDelay)
            throws ConnectException {
        final Metronome metronome = Metronome.parker(retryDelay, Clock.SYSTEM);
        short retryCount = 0;
        ReplicationConnection replicationConnection = null;
        while (retryCount <= maxRetries) {
            try {
                return taskContext.createReplicationConnection(doSnapshot);
            }
            catch (SQLException ex) {
                retryCount++;
                if (retryCount > maxRetries) {
                    LOGGER.error("Too many errors connecting to server." +
                            " All {} retries failed.", maxRetries);
                    throw new ConnectException(ex);
                }

                LOGGER.warn("Error connecting to server; will attempt retry {} of {} after {} " +
                        "seconds. Exception message: {}", retryCount,
                        maxRetries, retryDelay.getSeconds(), ex.getMessage());
                try {
                    metronome.pause();
                }
                catch (InterruptedException e) {
                    LOGGER.warn("Connection retry sleep interrupted by exception: " + e);
                    Thread.currentThread().interrupt();
                }
            }
        }
        return replicationConnection;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {

        final List<DataChangeEvent> records = queue.poll();
        LOGGER.info("SKSK doPoll Got the records from queue " + records);
        final List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    protected void doStop() {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }

        if (heartbeatConnection != null) {
            heartbeatConnection.close();
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return YugabyteDBConnectorConfig.ALL_FIELDS;
    }

    public YugabyteDBTaskContext getTaskContext() {
        return taskContext;
    }
}
