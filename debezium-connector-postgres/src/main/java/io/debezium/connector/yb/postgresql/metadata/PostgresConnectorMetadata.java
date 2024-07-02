/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yb.postgresql.metadata;

import io.debezium.config.Field;
import io.debezium.connector.yb.postgresql.Module;
import io.debezium.connector.yb.postgresql.PostgresConnector;
import io.debezium.connector.yb.postgresql.PostgresConnectorConfig;
import io.debezium.metadata.ConnectorDescriptor;
import io.debezium.metadata.ConnectorMetadata;

public class PostgresConnectorMetadata implements ConnectorMetadata {

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("postgres", "Debezium PostgreSQL Connector", PostgresConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getConnectorFields() {
        return PostgresConnectorConfig.ALL_FIELDS;
    }

}
