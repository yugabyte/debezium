/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import io.debezium.metadata.ConnectorDescriptor;
import org.apache.kafka.connect.connector.Connector;

import io.debezium.config.Field;
import io.debezium.metadata.ConnectorMetadata;

public class YugabyteDBConnectorMetadata implements ConnectorMetadata {

    @Override public Field.Set getConnectorFields() {
//        return null;
        return getAllConnectorFields();
    }

    @Override public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("", "yugabytedb", "Debezium YugabyteDB connector", getConnector().version());
    }

//    @Override
    public Connector getConnector() {
        return new YugabyteDBConnector();
    }

//    @Override
    public Field.Set getAllConnectorFields() {
        return YugabyteDBConnectorConfig.ALL_FIELDS;
    }

}
