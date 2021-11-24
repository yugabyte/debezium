/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.connector.yugabytedb.connection.PostgresConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * A Kafka Connect source connector that creates tasks which use Postgresql streaming replication off a logical replication slot
 * to receive incoming changes for a database and publish them to Kafka.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in {@link PostgresConnectorConfig}.
 *
 * @author Horia Chiorean
 */
public class PostgresConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresConnector.class);
    private Map<String, String> props;

    public PostgresConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PostgresConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // CDCSDK Need to create maxTasks
        // For now, just have 1 task, we will separate it in future
        // also need to divide the table and tablets across the maxTasks.
        // this will always have just one task with the given list of properties
        return props == null ? Collections.emptyList() : Collections.singletonList(new HashMap<>(props));
    }

    @Override
    public void stop() {
        this.props = null;
    }

    @Override
    public ConfigDef config() {
        return PostgresConnectorConfig.configDef();
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        final ConfigValue databaseValue = configValues.get(RelationalDatabaseConnectorConfig.DATABASE_NAME.name());
        if (!databaseValue.errorMessages().isEmpty()) {
            return;
        }

        final PostgresConnectorConfig postgresConfig = new PostgresConnectorConfig(config);
        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        // Try to connect to the database ...
        try (PostgresConnection connection = new PostgresConnection(postgresConfig.getJdbcConfig())) {
            try {
                // Prepare connection without initial statement execution
                connection.connection(false);
                // check connection
                connection.execute("SELECT version()");
                LOGGER.info("Successfully tested connection for {} with user '{}'", connection.connectionString(),
                        connection.username());
                // check server wal_level
                // TODO: CDCSDK No need to check wal_level
                // 1. check for the StreamID
                // check user for LOGIN and REPLICATION roles
                // TODO: CDCSDK We will check in future for user login and roles.
            }
            catch (SQLException e) {
                LOGGER.error("Failed testing connection for {} with user '{}'", connection.connectionString(),
                        connection.username(), e);
                hostnameValue.addErrorMessage("Error while validating connector config: " + e.getMessage());
            }
        }
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(PostgresConnectorConfig.ALL_FIELDS);
    }
}
