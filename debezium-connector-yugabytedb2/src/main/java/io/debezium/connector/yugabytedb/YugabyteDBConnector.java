/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

import com.google.common.net.HostAndPort;
import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.client.*;
import org.yb.master.MasterDdlOuterClass;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A Kafka Connect source connector that creates tasks which use YugabyteDB CDC API
 * to receive incoming changes for a database and publish them to Kafka.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in
 * {@link YugabyteDBConnectorConfig}.
 *
 * @author Suranjan Kumar (skumar@yugabyte.com)
 */
public class YugabyteDBConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBConnector.class);
    private Map<String, String> props;
    private YBClient ybClient;
    private volatile YugabyteDBConnection connection;
    private Set<String> tableIds;
    private List<Pair<String, String>> tabletIds;
    private YugabyteDBConnectorConfig yugabyteDBConnectorConfig;

    public YugabyteDBConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return YugabyteDBConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        LOGGER.debug("Props " + props);
        Configuration config = Configuration.from(this.props);
        this.yugabyteDBConnectorConfig = new YugabyteDBConnectorConfig(config);
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (props == null) {
            LOGGER.error("Configuring a maximum of {} tasks with no connector configuration" +
                    " available", maxTasks);
            return Collections.emptyList();
        }

        connection = new YugabyteDBConnection(yugabyteDBConnectorConfig.getJdbcConfig());
        final Charset databaseCharset = connection.getDatabaseCharset();
        String charSetName = databaseCharset.name();

        YugabyteDBTypeRegistry typeRegistry = new YugabyteDBTypeRegistry(connection);

        Map<String, YugabyteDBType> nameToType = typeRegistry.getNameToType();
        Map<Integer, YugabyteDBType> oidToType = typeRegistry.getOidToType();
        String serializedNameToType = "";
        try {
            serializedNameToType = ObjectUtil.serializeObjectToString(nameToType);
            LOGGER.debug("The serializedNameToType " + serializedNameToType);
            Object test = ObjectUtil.deserializeObjectFromString(serializedNameToType);
            LOGGER.debug("The deserializedNameToType " + test);
        }
        catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        String serializedOidToType = "";
        try {
            serializedOidToType = ObjectUtil.serializeObjectToString(oidToType);
            LOGGER.debug("The serializedOidToType " + serializedOidToType);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        Configuration config = Configuration.from(this.props);
        Map<String, ConfigValue> results = validateAllFields(config);

        validateTServerConnection(results, config);
        String streamIdValue = "";
        if (this.yugabyteDBConnectorConfig.streamId() == null) {
            streamIdValue = this.props.get(YugabyteDBConnectorConfig.STREAM_ID.toString());
        }

        LOGGER.info("The streamid being used is" + streamIdValue);

        int numGroups = Math.min(this.tabletIds.size(), maxTasks);
        LOGGER.info("The tabletIds size are " + tabletIds.size() + " maxTasks" + maxTasks);

        List<List<Pair<String, String>>> tabletIdsGrouped = ConnectorUtils.groupPartitions(this.tabletIds, numGroups);
        LOGGER.info("The grouped tabletIds are " + tabletIdsGrouped.size());
        List<Map<String, String>> taskConfigs = new ArrayList<>(tabletIdsGrouped.size());

        for (List<Pair<String, String>> taskTables : tabletIdsGrouped) {
            LOGGER.info("The taskTables are " + taskTables);
            Map<String, String> taskProps = new HashMap<>(this.props);
            int taskId = taskConfigs.size();
            taskProps.put(YugabyteDBConnectorConfig.TASK_ID.toString(), String.valueOf(taskId));
            String taskTablesSerialized = "";
            try {
                taskTablesSerialized = ObjectUtil.serializeObjectToString(taskTables);
                LOGGER.info("The taskTablesSerialized " + taskTablesSerialized);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            taskProps.put(YugabyteDBConnectorConfig.TABLET_LIST.toString(), taskTablesSerialized);
            taskProps.put(YugabyteDBConnectorConfig.CHAR_SET.toString(), charSetName);
            taskProps.put(YugabyteDBConnectorConfig.NAME_TO_TYPE.toString(), serializedNameToType);
            taskProps.put(YugabyteDBConnectorConfig.OID_TO_TYPE.toString(), serializedOidToType);
            taskProps.put(YugabyteDBConnectorConfig.STREAM_ID.toString(), streamIdValue);
            taskConfigs.add(taskProps);
        }

        LOGGER.debug("Configuring {} YugabyteDB connector task(s)", taskConfigs.size());
        closeYBClient();
        return taskConfigs;
    }

    private void closeYBClient() {
        try {
            ybClient.close();
        }
        catch (Exception e) {
            LOGGER.warn("Received exception while shutting down the client", e);
        }
    }

    private YBClient getYBClientBase(String hostAddress, long adminTimeout, long operationTimeout, long socketReadTimeout,
                                     int maxNumTablets, String certFile, String clientCert, String clientKey) {
        if (maxNumTablets == -1) {
            maxNumTablets = yugabyteDBConnectorConfig.maxNumTablets();
        }

        if (adminTimeout == -1) {
            adminTimeout = yugabyteDBConnectorConfig.adminOperationTimeoutMs();
        }

        if (operationTimeout == -1) {
            operationTimeout = yugabyteDBConnectorConfig.operationTimeoutMs();
        }

        if (socketReadTimeout == -1) {
            socketReadTimeout = yugabyteDBConnectorConfig.socketReadTimeoutMs();
        }

        AsyncYBClient asyncClient = new AsyncYBClient.AsyncYBClientBuilder(hostAddress)
                .defaultAdminOperationTimeoutMs(adminTimeout)
                .defaultOperationTimeoutMs(operationTimeout)
                .defaultSocketReadTimeoutMs(socketReadTimeout)
                .numTablets(maxNumTablets)
                .sslCertFile(certFile)
                .sslClientCertFiles(clientCert, clientKey)
                .build();

        return new YBClient(asyncClient);
    }

    private YBClient getYBClient(String hostAddress, long adminTimeout, long opTimeout,
                                 long socketTimeout) {
        return getYBClient(hostAddress, adminTimeout, opTimeout, socketTimeout, -1);
    }

    // over loaded function
    private YBClient getYBClient(String hostAddress, long adminTimeout, long opTimeout,
                                 long socketTimeout, int numTablets) {
        return getYBClientBase(hostAddress, adminTimeout, opTimeout, socketTimeout, numTablets, null, null, null);
    }

    @Override
    public void stop() {
        this.props = null;
        try {
            ybClient.close();
        }
        catch (Exception e) {
            LOGGER.warn("Received exception while shutting down the client", e);
        }
    }

    @Override
    public ConfigDef config() {
        return YugabyteDBConnectorConfig.configDef();
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        final ConfigValue databaseValue = configValues.get(RelationalDatabaseConnectorConfig.DATABASE_NAME.name());
        if (!databaseValue.errorMessages().isEmpty()) {
            return;
        }

        this.yugabyteDBConnectorConfig = new YugabyteDBConnectorConfig(config);
        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        // Try to connect to the database ...
        try (YugabyteDBConnection connection = new YugabyteDBConnection(yugabyteDBConnectorConfig
                .getJdbcConfig())) {
            try {
                // Prepare connection without initial statement execution
                connection.connection(false);
                // check connection
                connection.execute("SELECT version()");
                LOGGER.info("Successfully tested connection for {} with user '{}'",
                        connection.connectionString(),
                        connection.username());
            }
            catch (SQLException e) {
                LOGGER.error("Failed testing connection for {} with user '{}'",
                        connection.connectionString(),
                        connection.username(), e);
                hostnameValue.addErrorMessage("Error while validating connector config: "
                        + e.getMessage());
            }
        }

        validateTServerConnection(configValues, config);
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(YugabyteDBConnectorConfig.ALL_FIELDS);
    }

    protected void validateTServerConnection(Map<String, ConfigValue> configValues,
                                             Configuration config) {
        // TODO: CDCSDK We will check in future for user login and roles.
        String hostAddress = config.getString(YugabyteDBConnectorConfig.MASTER_ADDRESSES.toString());
        // todo vaibhav: check if the static variables can be replaced with their respective functions
        this.ybClient = getYBClientBase(hostAddress,
                yugabyteDBConnectorConfig.adminOperationTimeoutMs(),
                yugabyteDBConnectorConfig.operationTimeoutMs(),
                yugabyteDBConnectorConfig.socketReadTimeoutMs(),
                yugabyteDBConnectorConfig.maxNumTablets(),
                yugabyteDBConnectorConfig.sslRootCert(),
                yugabyteDBConnectorConfig.sslClientCert(),
                yugabyteDBConnectorConfig.sslClientKey()); // always passing the ssl root certs,
        // so whenever they are null, they will just be ignored
        // todo vaibhav: remove this comment
        // this.ybClient = getYBClient(hostAddress, 60000,
        // 60000, 60000);
        LOGGER.debug("The master host address is " + hostAddress);
        HostAndPort masterHostPort = ybClient.getLeaderMasterHostAndPort();
        if (masterHostPort == null) {
            LOGGER.error("Failed testing connection at {}", yugabyteDBConnectorConfig.hostname());
        }

        // do a get and check if the streamid exists.
        // TODO: Suranjan check the db stream info and verify if the tableIds are present
        // TODO: Find out where to do validation for table whitelist
        ConfigValue streamId = configValues.get(YugabyteDBConnectorConfig.STREAM_ID);
        String streamIdValue = this.props.get(YugabyteDBConnectorConfig.STREAM_ID);

        this.tableIds = fetchTabletList();

        if (tableIds.isEmpty()) {
            LOGGER.info(String.format("The table id is empty."));
            System.exit(1);
        }

        if (streamIdValue == null || streamIdValue.isEmpty()) {
            // Create stream.
            String tableid = tableIds.stream().findFirst().get();
            try {
                YBTable t = this.ybClient.openTableByUUID(tableid);
                streamIdValue = this.ybClient.createCDCStream(t, yugabyteDBConnectorConfig.databaseName(),
                        "PROTO",
                        "IMPLICIT").getStreamId();
                this.props.put(yugabyteDBConnectorConfig.STREAM_ID.toString(), streamIdValue);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            LOGGER.info(String.format("Created a new stream ID: %s", streamId));
        }
        try {
            // TODO: Need to change for tableid here otherwise it will not work.
            GetDBStreamInfoResponse res = this.ybClient.getDBStreamInfo(streamIdValue);
            if (res.getTableInfoList().isEmpty()) {
                LOGGER.info("The table info is empty!");
            }
            // Get all the table_ids
        }
        catch (Exception e) {
            LOGGER.error("Failed fetching all tables for the streamid {} ",
                    streamId, e);
            streamId.addErrorMessage("Failed fetching all tables for the streamid: "
                    + e.getMessage());
        }

        this.tabletIds = new ArrayList<>();
        try {
            for (String tableId : tableIds) {
                YBTable table = ybClient.openTableByUUID(tableId);
                this.tabletIds.addAll(ybClient.getTabletUUIDs(table).stream()
                        .map(tabletId -> new ImmutablePair<String,String>(tableId, tabletId))
                        .collect(Collectors.toList()));
            }
        }
        catch (Exception e) {
            LOGGER.error("Error while fetching all the tablets", e);
        }
    }

    Set<String> fetchTabletList() {
        LOGGER.info("Fetching tables");
        Set<String> tIds = new HashSet<>();
        try {
            ListTablesResponse tablesResp = this.ybClient.getTablesList();
            for (MasterDdlOuterClass.ListTablesResponsePB.TableInfo tableInfo : tablesResp.getTableInfoList()) {
                String fqlTableName = tableInfo.getNamespace().getName() + "." + tableInfo.getPgschemaName()
                        + "." + tableInfo.getName();
                LOGGER.info("VKVK fqlTableName is " + fqlTableName);
                TableId tableId = YugabyteDBSchema.parseWithSchema(fqlTableName, tableInfo.getPgschemaName());
                if (yugabyteDBConnectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                    LOGGER.info(
                            "VKVK adding table ID: " + tableInfo.getId() + " of table: " + tableInfo.getName() + " in namespace: " + tableInfo.getNamespace().getName());
                    tIds.add(tableInfo.getId().toStringUtf8());
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return tIds;
    }
}
