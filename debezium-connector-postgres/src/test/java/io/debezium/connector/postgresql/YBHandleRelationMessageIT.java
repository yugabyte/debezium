/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;

/**
 * Integration tests for PK resolution in {@code PgOutputMessageDecoder.handleRelationMessage}.
 *
 * Validates that Kafka record keys are correctly resolved for each replica identity
 * (DEFAULT, FULL, CHANGE), including a table-drop scenario where the connector is
 * stopped, the table is dropped, and the connector resumes processing pending WAL events.
 *
 * @author Shishir Sharma (ssharma@yugabyte.com)
 */
public class YBHandleRelationMessageIT extends AbstractConnectorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(YBHandleRelationMessageIT.class);

    private static final String TABLE_NAME = "pk_test";
    private static final String QUALIFIED_TABLE = "public." + TABLE_NAME;
    private static final String CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + " (pk SERIAL PRIMARY KEY, val INTEGER);";
    private static final String PUBLICATION_NAME = "dbz_publication";

    @BeforeClass
    public static void beforeClass() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @Before
    public void before() {
        initializeConnectorTestFramework();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
        TestHelper.execute("DROP TABLE IF EXISTS " + TABLE_NAME + ";");
        TestHelper.execute(CREATE_TABLE);
    }

    @After
    public void after() {
        stopConnector();
        try {
            TestHelper.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity " +
                    "WHERE pid <> pg_backend_pid() " +
                    "AND (backend_type='walsender' OR application_name LIKE 'Debezium%')");
            Thread.sleep(5_000);
        }
        catch (Exception e) {
            LOGGER.warn("Failed to terminate backends during teardown", e);
        }
        try {
            TestHelper.dropDefaultReplicationSlot();
        }
        catch (Exception e) {
            LOGGER.warn("Failed to drop replication slot during teardown", e);
        }
        TestHelper.dropPublication();
    }

    @Test
    public void shouldResolvePkWithReplicaIdentityDefault() throws Exception {
        verifyPkResolution("DEFAULT");
    }

    @Test
    public void shouldResolvePkWithReplicaIdentityFull() throws Exception {
        verifyPkResolution("FULL");
    }

    @Test
    public void shouldResolvePkWithReplicaIdentityChange() throws Exception {
        verifyPkResolution("CHANGE");
    }

    @Test
    public void shouldResolvePkAfterTableDropWithReplicaIdentityDefault() throws Exception {
        verifyPkAfterTableDrop("DEFAULT");
    }

    @Test
    public void shouldResolvePkAfterTableDropWithReplicaIdentityFull() throws Exception {
        verifyPkAfterTableDrop("FULL");
    }

    @Test
    public void shouldResolvePkAfterTableDropWithReplicaIdentityChange() throws Exception {
        verifyPkAfterTableDrop("CHANGE");
    }

    /**
     * Verifies that PK columns are correctly resolved during normal streaming
     * for the given replica identity.
     */
    private void verifyPkResolution(String replicaIdentity) throws Exception {
        TestHelper.execute("ALTER TABLE " + TABLE_NAME + " REPLICA IDENTITY " + replicaIdentity + ";");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .build();

        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitFor(Duration.ofSeconds(10));
        waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
        assertNoRecordsToConsume();

        TestHelper.execute(
                "INSERT INTO " + TABLE_NAME + " (val) VALUES (10);" +
                "INSERT INTO " + TABLE_NAME + " (val) VALUES (20);" +
                "INSERT INTO " + TABLE_NAME + " (val) VALUES (30);");

        SourceRecords records = consumeRecordsByTopic(3);
        List<SourceRecord> tableRecords = records.recordsForTopic(topicName(QUALIFIED_TABLE));

        assertThat(tableRecords).hasSize(3);
        for (int i = 0; i < tableRecords.size(); i++) {
            SourceRecord record = tableRecords.get(i);
            assertThat(record.key()).as("Kafka key should not be null for record %d", i).isNotNull();
            assertThat(record.keySchema().fields()).as("Key schema should have fields").isNotEmpty();
            YBVerifyRecord.isValidInsert(record, PK_FIELD, i + 1);
        }
    }

    /**
     * Verifies that PK columns survive a table-drop scenario: the connector is stopped,
     * rows are inserted, the table is dropped from the publication and then dropped entirely,
     * the walsender is killed, and the connector is restarted to process the pending WAL events.
     */
    private void verifyPkAfterTableDrop(String replicaIdentity) throws Exception {
        TestHelper.execute("ALTER TABLE " + TABLE_NAME + " REPLICA IDENTITY " + replicaIdentity + ";");

        // Create a table-specific publication so we can later DROP TABLE from it.
        // FOR ALL TABLES publications do not allow per-table removal.
        TestHelper.execute("CREATE PUBLICATION " + PUBLICATION_NAME + " FOR TABLE " + TABLE_NAME + ";");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE,
                        PostgresConnectorConfig.AutoCreateMode.DISABLED.getValue())
                .build();

        // Start connector, insert a baseline row, verify it has a valid key
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitFor(Duration.ofSeconds(10));
        waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
        assertNoRecordsToConsume();

        TestHelper.execute("INSERT INTO " + TABLE_NAME + " (val) VALUES (100);");

        SourceRecords baseline = consumeRecordsByTopic(1);
        List<SourceRecord> baselineRecords = baseline.recordsForTopic(topicName(QUALIFIED_TABLE));
        assertThat(baselineRecords).hasSize(1);
        assertThat(baselineRecords.get(0).key()).as("Baseline key should not be null").isNotNull();
        assertThat(baselineRecords.get(0).keySchema().fields()).as("Baseline key schema should have fields").isNotEmpty();
        YBVerifyRecord.isValidInsert(baselineRecords.get(0), true);

        // Stop connector
        stopConnector();
        Thread.sleep(2_000);

        // Insert rows while connector is down
        int pendingRows = 5;
        StringBuilder insertBatch = new StringBuilder();
        for (int i = 0; i < pendingRows; i++) {
            insertBatch.append("INSERT INTO " + TABLE_NAME + " (val) VALUES (" + (200 + i) + ");");
        }
        TestHelper.execute(insertBatch.toString());

        Thread.sleep(1_000);

        // Drop table from publication, then drop the table itself
        TestHelper.execute("ALTER PUBLICATION " + PUBLICATION_NAME + " DROP TABLE " + TABLE_NAME + ";");
        TestHelper.execute("DROP TABLE " + TABLE_NAME + ";");

        // Kill the walsender to force a fresh connection on restart
        TestHelper.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type='walsender'");
        Thread.sleep(10_000);

        // Restart connector and consume the pending rows
        start(YugabyteDBConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitFor(Duration.ofSeconds(10));

        SourceRecords pending = consumeRecordsByTopic(pendingRows);
        List<SourceRecord> pendingRecords = pending.recordsForTopic(topicName(QUALIFIED_TABLE));

        assertThat(pendingRecords).as("Should receive all %d pending records", pendingRows).hasSize(pendingRows);
        for (int i = 0; i < pendingRecords.size(); i++) {
            SourceRecord record = pendingRecords.get(i);
            assertThat(record.key()).as("Key should not be null for pending record %d", i).isNotNull();
            assertThat(record.keySchema().fields()).as("Key schema should have fields for record %d", i).isNotEmpty();
            YBVerifyRecord.isValidInsert(record, true);
        }
    }
}
