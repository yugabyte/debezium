/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.yugabytedb.connection.PostgresConnection;
import io.debezium.connector.yugabytedb.connection.ReplicationConnection;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

/**
 * The context of a {@link PostgresConnectorTask}. This deals with most of the brunt of reading various configuration options
 * and creating other objects with these various options.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
@ThreadSafe
public class PostgresTaskContext extends CdcSourceTaskContext {

    protected final static Logger LOGGER = LoggerFactory.getLogger(PostgresTaskContext.class);

    private final PostgresConnectorConfig config;
    private final TopicSelector<TableId> topicSelector;
    private final PostgresSchema schema;

    protected PostgresTaskContext(PostgresConnectorConfig config, PostgresSchema schema, TopicSelector<TableId> topicSelector) {
        super(config.getContextName(), config.getLogicalName(), Collections::emptySet);

        this.config = config;
        this.topicSelector = topicSelector;
        assert schema != null;
        this.schema = schema;
    }

    protected TopicSelector<TableId> topicSelector() {
        return topicSelector;
    }

    protected PostgresSchema schema() {
        return schema;
    }

    protected PostgresConnectorConfig config() {
        return config;
    }

    protected void refreshSchema(PostgresConnection connection, boolean printReplicaIdentityInfo) throws SQLException {
        schema.refresh(connection, printReplicaIdentityInfo);
    }

    protected ReplicationConnection createReplicationConnection(boolean doSnapshot) throws SQLException {
        /*
         * final boolean dropSlotOnStop = config.dropSlotOnStop();
         * if (dropSlotOnStop) {
         * LOGGER.warn(
         * "Connector has enabled automated replication slot removal upon restart ({} = true). " +
         * "This setting is not recommended for production environments, as a new replication slot " +
         * "will be created after a connector restart, resulting in missed data change events.",
         * PostgresConnectorConfig.DROP_SLOT_ON_STOP.name());
         * }
         */
        return ReplicationConnection.builder(config)
                // .withSlot(config.slotName())
                // .withPublication(config.publicationName())
                .withTableFilter(config.getTableFilters())
                // .withPublicationAutocreateMode(config.publicationAutocreateMode())
                .withPlugin(config.plugin())
                .withTruncateHandlingMode(config.truncateHandlingMode())
                // .dropSlotOnClose(dropSlotOnStop)
                .streamParams(config.streamParams())
                .statusUpdateInterval(config.statusUpdateInterval())
                .withTypeRegistry(schema.getTypeRegistry())
                .doSnapshot(doSnapshot)
                .withSchema(schema)
                .build();
    }

    PostgresConnectorConfig getConfig() {
        return config;
    }
}
