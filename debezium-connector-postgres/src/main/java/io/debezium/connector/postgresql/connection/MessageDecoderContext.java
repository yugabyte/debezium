/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.YugabyteDBVersion;

/**
 * Contextual data required by {@link MessageDecoder}s.
 *
 * @author Chris Cranford
 */
public class MessageDecoderContext {

    private final PostgresConnectorConfig config;
    private final PostgresSchema schema;
    private volatile YugabyteDBVersion yugabyteDBVersion = YugabyteDBVersion.UNKNOWN;

    public MessageDecoderContext(PostgresConnectorConfig config, PostgresSchema schema) {
        this.config = config;
        this.schema = schema;
    }

    public PostgresConnectorConfig getConfig() {
        return config;
    }

    public PostgresSchema getSchema() {
        return schema;
    }

    /**
     * @return the YugabyteDB server version of the node serving the replication stream. It is read
     *         from the streaming connection itself when streaming starts (and re-read on every
     *         reconnect), because the relation-message format and {@code version()} move together
     *         per node — so this stays correct even mid-rolling-upgrade, when the metadata
     *         connection may be on a different-version node. Defaults to
     *         {@link YugabyteDBVersion#UNKNOWN} until streaming starts (UNKNOWN keeps the safe
     *         pgoutput PK fallback enabled).
     */
    public YugabyteDBVersion getYugabyteDBVersion() {
        return yugabyteDBVersion;
    }

    /**
     * Sets the YugabyteDB server version once it has been resolved from the streaming connection.
     */
    public void setYugabyteDBVersion(YugabyteDBVersion yugabyteDBVersion) {
        this.yugabyteDBVersion = yugabyteDBVersion;
    }
}
