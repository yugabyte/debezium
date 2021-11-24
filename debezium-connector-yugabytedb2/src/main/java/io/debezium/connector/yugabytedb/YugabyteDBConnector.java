/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.*;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;
import org.yb.client.AsyncYBClient;
import org.yb.client.YBClient;

import com.google.common.net.HostAndPort;

import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

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
    private List<CdcService.TableInfo> tableInfoList;
    private volatile YugabyteDBConnection connection;
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
        LOGGER.info("Props " + props);
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
            LOGGER.info("The serializedNameToType " + serializedNameToType);
            Object test = ObjectUtil.deserializeObjectFromString(serializedNameToType);
            LOGGER.info("The deserializedNameToType " + test);
        }
        catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        String serializedOidToType = "";
        try {
            serializedOidToType = ObjectUtil.serializeObjectToString(oidToType);
            LOGGER.info("The serializedOidToType " + serializedOidToType);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        Configuration config = Configuration.from(this.props);
        Map<String, ConfigValue> results = validateAllFields(config);

        validateTServerConnection(results, config);

        // List<String> tableIds = this.tableInfoList.stream().map(tableInfo -> tableInfo
        // .getTableId().toStringUtf8())
        // .collect(Collectors.toList());
        //
        // // TODO: Suranjan Use the table whitelist here
        // List<Pair<String, String>> tabletIds = new ArrayList<>();
        // try {
        // for (String tableId : tableIds) {
        // YBTable table = ybClient.openTableByUUID(tableId);
        // tabletIds.addAll(ybClient.getTabletUUIDs(table).stream()
        // .map(tabletId -> new Pair<>(tableId, tabletId))
        // .collect(Collectors.toList()));
        // }
        // }
        // catch (Exception e) {
        // LOGGER.error("Error while fetching all the tablets", e);
        // }
        //
        // int numGroups = Math.min(tableIds.size(), maxTasks);
        // List<List<Pair<String, String>>> tabletIdsGrouped = ConnectorUtils
        // .groupPartitions(tabletIds, numGroups);
        // List<Map<String, String>> taskConfigs = new ArrayList<>(tabletIdsGrouped.size());
        //
        // // TODO: Suranjan also see how to put yugabytedb type map
        // for (List<Pair<String, String>> taskTables : tabletIdsGrouped) {
        // LOGGER.info("The taskTables are " + taskTables);
        // Map<String, String> taskProps = new HashMap<>(props);
        // taskProps.put(YugabyteDBConnectorConfig.TABLET_LIST.toString(), taskTables.toString());
        // taskProps.put(YugabyteDBConnectorConfig.CHAR_SET.toString(), charSetName);
        // taskProps.put(YugabyteDBConnectorConfig.NAME_TO_TYPE.toString(), serializedNameToType);
        // taskProps.put(YugabyteDBConnectorConfig.OID_TO_TYPE.toString(), serializedOidToType);
        // taskConfigs.add(taskProps);
        // }

        List<Map<String, String>> taskConfigs = new ArrayList<>(1);

        // TODO: Suranjan also see how to put yugabytedb type map
        // for (List<Pair<String, String>> taskTables : tabletIdsGrouped) {
        // LOGGER.info("The taskTables are " + taskTables);
        Map<String, String> taskProps = new HashMap<>(props);
        // taskProps.put(YugabyteDBConnectorConfig.TABLET_LIST.toString(), taskTables.toString
        // ());
        taskProps.put(YugabyteDBConnectorConfig.CHAR_SET.toString(), charSetName);
        taskProps.put(YugabyteDBConnectorConfig.NAME_TO_TYPE.toString(), serializedNameToType);
        taskProps.put(YugabyteDBConnectorConfig.OID_TO_TYPE.toString(), serializedOidToType);
        taskConfigs.add(taskProps);
        // }
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

    private YBClient getYBClient(String hostAddress, long adminTimeout, long opTimeout,
                                 long socketTimeout) {
        AsyncYBClient client = new AsyncYBClient.AsyncYBClientBuilder(hostAddress)
                .defaultAdminOperationTimeoutMs(adminTimeout)
                .defaultOperationTimeoutMs(opTimeout)
                .defaultSocketReadTimeoutMs(socketTimeout)
                .build();

        YBClient syncClient = new YBClient(client);
        return syncClient;
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

        // validateTServerConnection(configValues, config);
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(YugabyteDBConnectorConfig.ALL_FIELDS);
    }

    protected void validateTServerConnection(Map<String, ConfigValue> configValues,
                                             Configuration config) {
        ConfigValue hostnameValue = configValues.get(YugabyteDBConnectorConfig.MASTER_HOSTNAME.name());
        ConfigValue portValue = configValues.get(YugabyteDBConnectorConfig.MASTER_PORT.name());
        String hostname = this.props.get(YugabyteDBConnectorConfig.MASTER_HOSTNAME.toString());
        int port = Integer.parseInt(this.props.get(YugabyteDBConnectorConfig.MASTER_PORT.toString()));
        // TODO: Suranjan Check for timeout, socket timeout property
        // TODO: Suranjan check for SSL property too and validate if set
        // TODO: CDCSDK We will check in future for user login and roles.
        String hostAddress = "" + hostname + ":" + port;
        this.ybClient = getYBClient(hostAddress, 60000,
                60000, 60000);
        LOGGER.debug("The master host address is " + hostAddress);
        HostAndPort masterHostPort = ybClient.getLeaderMasterHostAndPort();
        if (masterHostPort == null) {
            LOGGER.error("Failed testing connection for {} with address '{}'",
                    hostnameValue.value(),
                    portValue.value());
        }

        // do a get and check if the streamid exists.
        // TODO: Suranjan check the db stream info and verify if the tableIds are present
        // TODO: Find out where to do validation for table whitelist
        // ConfigValue streamId = configValues.get(YugabyteDBConnectorConfig.STREAM_ID);
        // String streamIdValue = this.props.get(YugabyteDBConnectorConfig.STREAM_ID);
        // try {
        // // TODO: Need to change for tableid here otherwise it will not work.
        // GetDBStreamInfoResponse res = this.ybClient.getDBStreamInfo(null,
        // streamIdValue);
        // this.tableInfoList = res.getTableInfoList();
        // }
        // catch (Exception e) {
        // LOGGER.error("Failed fetching all tables for the streamid {} ",
        // streamId, e);
        // streamId.addErrorMessage("Failed fetching all tables for the streamid: "
        // + e.getMessage());
        // }
    }
}
