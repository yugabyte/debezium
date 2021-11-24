/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.time.Duration;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Test;

import io.debezium.connector.yugabytedb.connection.ReplicationConnection;
import io.debezium.doc.FixFor;

/**
 * Integration test for {@link YugabyteDBConnectorTask} class.
 */
public class YugabyteDBConnectorTaskIT {

    @Test
    @FixFor("DBZ-519")
    public void shouldNotThrowNullPointerExceptionDuringCommit() throws Exception {
        YugabyteDBConnectorTask yugabyteDBConnectorTask = new YugabyteDBConnectorTask();
        yugabyteDBConnectorTask.commit();
    }

    class FakeContext extends YugabyteDBTaskContext {
        public FakeContext(YugabyteDBConnectorConfig yugabyteDBConnectorConfig, YugabyteDBSchema yugabyteDBSchema) {
            super(yugabyteDBConnectorConfig, yugabyteDBSchema, null);
        }

        @Override
        protected ReplicationConnection createReplicationConnection(boolean doSnapshot) throws SQLException {
            throw new SQLException("Could not connect");
        }
    }

    @Test(expected = ConnectException.class)
    @FixFor("DBZ-1426")
    public void retryOnFailureToCreateConnection() throws Exception {
        YugabyteDBConnectorTask yugabyteDBConnectorTask = new YugabyteDBConnectorTask();
        YugabyteDBConnectorConfig config = new YugabyteDBConnectorConfig(TestHelper.defaultConfig().build());
        long startTime = System.currentTimeMillis();
        yugabyteDBConnectorTask.createReplicationConnection(new FakeContext(config, new YugabyteDBSchema(
                config,
                null,
                PostgresTopicSelector.create(config), null)), true, 3, Duration.ofSeconds(2));

        // Verify retry happened for 10 seconds
        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;
        Assert.assertTrue(timeElapsed > 5);
    }
}
