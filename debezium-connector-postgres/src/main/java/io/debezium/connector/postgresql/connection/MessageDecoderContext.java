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
    private final YugabyteDBVersion yugabyteDBVersion;

    public MessageDecoderContext(PostgresConnectorConfig config, PostgresSchema schema, YugabyteDBVersion yugabyteDBVersion) {
        this.config = config;
        this.schema = schema;
        this.yugabyteDBVersion = yugabyteDBVersion;
    }

    public PostgresConnectorConfig getConfig() {
        return config;
    }

    public PostgresSchema getSchema() {
        return schema;
    }

    /**
     * @return the YugabyteDB server version, resolved once (with retry) when the replication
     *         connection was created; connector start-up fails earlier if it cannot be determined.
     *         This is per-connection (hence per-task) state, so connectors targeting different
     *         clusters in the same JVM each see their own version.
     */
    public YugabyteDBVersion getYugabyteDBVersion() {
        return yugabyteDBVersion;
    }
}
