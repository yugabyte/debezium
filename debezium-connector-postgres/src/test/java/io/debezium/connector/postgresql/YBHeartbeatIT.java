/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.embedded.EmbeddedEngineConfig;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.heartbeat.Heartbeat;

/**
 * Integration tests for YB streaming-phase heartbeat behaviour.
 *
 * @author bakul-gupta
 */
public class YBHeartbeatIT extends AbstractConnectorTest {

    // Cap engine shutdown at 30s (default is 5min) so tests stay responsive.
    private static final long SHUTDOWN_PAUSE_MS = 30_000L;

    // YB MBeans have extra tags that waitForStreamingRunning can't match; use a static wait.
    private static final Duration STREAMING_STARTUP_WAIT = Duration.ofSeconds(15);

    @Before
    public void before() throws SQLException {
        TestHelper.dropAllSchemas();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
        initializeConnectorTestFramework();
    }

    @After
    public void after() {
        try {
            stopConnector();
        }
        catch (Throwable ignored) {
        }
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    @Test
    public void test1_actionQueryRunsAndHeartbeatRecordReachesKafka() throws Exception {
        TestHelper.execute(
                "DROP SCHEMA IF EXISTS s1 CASCADE;" +
                        "CREATE SCHEMA s1;" +
                        "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                        "CREATE TABLE s1.heartbeat (ts TIMESTAMP WITH TIME ZONE PRIMARY KEY);" +
                        "INSERT INTO s1.heartbeat (ts) VALUES (NOW());");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.a")
                .with(Heartbeat.HEARTBEAT_INTERVAL, 500)
                .with(DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY, "UPDATE s1.heartbeat SET ts=NOW();")
                .with(EmbeddedEngineConfig.WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_MS, SHUTDOWN_PAUSE_MS)
                .build();

        final String initialTs = readHeartbeatTs();
        assertNotNull("Expected initial heartbeat row from setup", initialTs);

        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitFor(STREAMING_STARTUP_WAIT);

        TestHelper.execute("INSERT INTO s1.a (aa) VALUES (1);");

        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(500))
                .ignoreExceptions()
                .untilAsserted(() -> assertThat(readHeartbeatTs()).isNotEqualTo(initialTs));

        SourceRecord heartbeatRecord = pollForFirstHeartbeatRecord(Duration.ofSeconds(30));
        assertNotNull("Expected a heartbeat record on " + TestHelper.getDefaultHeartbeatTopic(),
                heartbeatRecord);
        assertThat(heartbeatRecord.valueSchema().name()).endsWith(".Heartbeat");
    }

    @Test
    public void test2_actionQueryTargetTableNotInIncludeList_lsnAdvances() throws Exception {
        TestHelper.execute(
                "DROP SCHEMA IF EXISTS s1 CASCADE;" +
                        "CREATE SCHEMA s1;" +
                        "CREATE TABLE s1.included (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                        "CREATE TABLE s1.heartbeat (ts TIMESTAMP WITH TIME ZONE PRIMARY KEY);" +
                        "INSERT INTO s1.heartbeat (ts) VALUES (NOW());");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.included")
                .with(Heartbeat.HEARTBEAT_INTERVAL, 500)
                .with(DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY, "UPDATE s1.heartbeat SET ts=NOW();")
                .with(EmbeddedEngineConfig.WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_MS, SHUTDOWN_PAUSE_MS)
                .build();

        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitFor(STREAMING_STARTUP_WAIT);

        TestHelper.execute("INSERT INTO s1.included (aa) VALUES (1);");
        consumeRecord();

        assertLsnAdvances();
    }

    @Test
    public void test3_externalActivityOnExcludedTables_lsnAdvances() throws Exception {
        TestHelper.execute(
                "DROP SCHEMA IF EXISTS s1 CASCADE;" +
                        "CREATE SCHEMA s1;" +
                        "CREATE TABLE s1.t1 (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                        "CREATE TABLE s1.t2 (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                        "CREATE TABLE s1.t3 (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                        "CREATE TABLE s1.t4 (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                        "CREATE TABLE s1.t5 (pk SERIAL, aa integer, PRIMARY KEY(pk));");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.t1")
                .with(Heartbeat.HEARTBEAT_INTERVAL, 500)
                .with(EmbeddedEngineConfig.WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_MS, SHUTDOWN_PAUSE_MS)
                .build();

        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitFor(STREAMING_STARTUP_WAIT);

        TestHelper.execute("INSERT INTO s1.t1 (aa) VALUES (1);");
        consumeRecord();

        final AtomicBoolean keepWriting = new AtomicBoolean(true);
        Thread writer = new Thread(() -> {
            int i = 0;
            while (keepWriting.get()) {
                try {
                    TestHelper.execute(
                            "INSERT INTO s1.t2 (aa) VALUES (" + i + ");" +
                                    "INSERT INTO s1.t3 (aa) VALUES (" + i + ");" +
                                    "INSERT INTO s1.t4 (aa) VALUES (" + i + ");" +
                                    "INSERT INTO s1.t5 (aa) VALUES (" + i + ");");
                    i++;
                    Thread.sleep(500);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                catch (Exception ignored) {
                }
            }
        }, "yb-hb-excluded-writer");
        writer.setDaemon(true);
        writer.start();

        try {
            assertLsnAdvances();
        }
        finally {
            keepWriting.set(false);
            writer.interrupt();
            writer.join(5_000);
        }
    }

    private void assertLsnAdvances() throws Exception {
        final Set<String> observedLsns = new HashSet<>();
        try (PostgresConnection connection = TestHelper.create()) {
            observedLsns.add(getConfirmedFlushLsn(connection));
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .ignoreExceptions()
                    .until(() -> observedLsns.add(getConfirmedFlushLsn(connection)));
        }
        assertThat(observedLsns.size())
                .as("confirmed_flush_lsn should advance via heartbeats")
                .isGreaterThan(1);
    }

    private String readHeartbeatTs() throws SQLException {
        try (PostgresConnection connection = TestHelper.create()) {
            return connection.queryAndMap("SELECT ts::text FROM s1.heartbeat LIMIT 1;", rs -> {
                if (rs.next()) {
                    return rs.getString(1);
                }
                return null;
            });
        }
    }

    private SourceRecord pollForFirstHeartbeatRecord(Duration timeout) throws InterruptedException {
        final String heartbeatTopic = TestHelper.getDefaultHeartbeatTopic();
        final long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadlineMs) {
            SourceRecord record = consumeRecord();
            if (record != null && heartbeatTopic.equals(record.topic())) {
                return record;
            }
        }
        return null;
    }

    private String getConfirmedFlushLsn(PostgresConnection connection) throws SQLException {
        final String lsn = connection.prepareQueryAndMap(
                "select * from pg_replication_slots where slot_name = ? and database = ? and plugin = ?",
                statement -> {
                    statement.setString(1, ReplicationConnection.Builder.DEFAULT_SLOT_NAME);
                    statement.setString(2, "yugabyte");
                    statement.setString(3, TestHelper.decoderPlugin().getPostgresPluginName());
                },
                rs -> {
                    if (rs.next()) {
                        return rs.getString("confirmed_flush_lsn");
                    }
                    fail("No replication slot info available");
                    return null;
                });
        connection.rollback();
        return lsn;
    }
}
