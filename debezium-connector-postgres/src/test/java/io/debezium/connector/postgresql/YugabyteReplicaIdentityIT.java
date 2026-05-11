package io.debezium.connector.postgresql;

import ch.qos.logback.classic.Level;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests to validate the functionality of replica identities with YugabyteDB.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteReplicaIdentityIT extends AbstractConnectorTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteReplicaIdentityIT.class);

  private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
     "DROP SCHEMA IF EXISTS s2 CASCADE;" +
     "CREATE SCHEMA s1; " +
     "CREATE SCHEMA s2; " +
     "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
     "CREATE TABLE s2.a (pk SERIAL, aa integer, bb varchar(20), PRIMARY KEY(pk));";

  private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
     "INSERT INTO s2.a (aa) VALUES (1);";

  private YugabyteDBConnector connector;

  @BeforeClass
  public static void beforeClass() throws SQLException {
    TestHelper.dropAllSchemas();
  }

  @Before
  public void before() throws InterruptedException {
    initializeConnectorTestFramework();
    terminateAndDropSlot();
    TestHelper.execute(CREATE_TABLES_STMT);
  }

  @After
  public void after() throws InterruptedException {
    stopConnector();
    terminateAndDropSlot();
    TestHelper.dropPublication();
  }

  private void terminateAndDropSlot() throws InterruptedException {
    for (int attempt = 0; attempt < 3; attempt++) {
      try {
        TestHelper.execute(
            "SELECT pg_terminate_backend(active_pid) "
                + "FROM pg_replication_slots "
                + "WHERE slot_name = 'debezium' AND active = true");
        TestHelper.waitFor(Duration.ofSeconds(3));
        TestHelper.dropDefaultReplicationSlot();
        return;
      }
      catch (Exception e) {
        if (attempt < 2) {
          TestHelper.waitFor(Duration.ofSeconds(5));
        }
        else {
          LOGGER.warn("Failed to drop replication slot after 3 attempts: {}", e.getMessage());
        }
      }
    }
  }

  @Test
  public void oldValuesWithReplicaIdentityFullForPgOutput() throws Exception {
    shouldProduceOldValuesWithReplicaIdentityFull(PostgresConnectorConfig.LogicalDecoder.PGOUTPUT);
  }

  @Test
  public void oldValuesWithReplicaIdentityFullForYbOutput() throws Exception {
    shouldProduceOldValuesWithReplicaIdentityFull(PostgresConnectorConfig.LogicalDecoder.YBOUTPUT);
  }

  public void shouldProduceOldValuesWithReplicaIdentityFull(PostgresConnectorConfig.LogicalDecoder logicalDecoder) throws Exception {
    TestHelper.execute("ALTER TABLE s1.a REPLICA IDENTITY FULL;");
    TestHelper.execute("ALTER TABLE s2.a REPLICA IDENTITY FULL;");

    Configuration config = TestHelper.defaultConfig()
                             .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                             .with(PostgresConnectorConfig.PLUGIN_NAME, logicalDecoder.getPostgresPluginName())
                             .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                             .build();
    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();

    // YB Note: Added a wait for replication slot to be active.
    TestHelper.waitFor(Duration.ofSeconds(10));

    waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
    // there shouldn't be any snapshot records
    assertNoRecordsToConsume();

    // insert and verify 2 new records
    TestHelper.execute(INSERT_STMT);
    TestHelper.execute("UPDATE s1.a SET aa = 12345 WHERE pk = 1;");

    SourceRecords actualRecords = consumeRecordsByTopic(3);
    List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s1.a"));

    SourceRecord insertRecord = records.get(0);
    SourceRecord updateRecord = records.get(1);

    if (logicalDecoder.isYBOutput()) {
      YBVerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
      YBVerifyRecord.isValidUpdate(updateRecord, PK_FIELD, 1);
    } else {
      VerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
      VerifyRecord.isValidUpdate(updateRecord, PK_FIELD, 1);
    }

    Struct updateRecordValue = (Struct) updateRecord.value();
    assertThat(updateRecordValue.get(Envelope.FieldName.AFTER)).isNotNull();
    assertThat(updateRecordValue.get(Envelope.FieldName.BEFORE)).isNotNull();

    if (logicalDecoder.isYBOutput()) {
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("aa").getInt32("value")).isEqualTo(1);
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).getStruct("aa").getInt32("value")).isEqualTo(12345);
    } else {
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.BEFORE).get("aa")).isEqualTo(1);
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).get("aa")).isEqualTo(12345);
    }
  }

  @Test
  public void replicaIdentityDefaultWithPgOutput() throws Exception {
    shouldProduceExpectedValuesWithReplicaIdentityDefault(PostgresConnectorConfig.LogicalDecoder.PGOUTPUT);
  }

  @Test
  public void replicaIdentityDefaultWithYbOutput() throws Exception {
    shouldProduceExpectedValuesWithReplicaIdentityDefault(PostgresConnectorConfig.LogicalDecoder.YBOUTPUT);
  }

  public void shouldProduceExpectedValuesWithReplicaIdentityDefault(PostgresConnectorConfig.LogicalDecoder logicalDecoder) throws Exception {
    TestHelper.execute("ALTER TABLE s1.a REPLICA IDENTITY DEFAULT;");
    TestHelper.execute("ALTER TABLE s2.a REPLICA IDENTITY DEFAULT;");

    Configuration config = TestHelper.defaultConfig()
                             .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                             .with(PostgresConnectorConfig.PLUGIN_NAME, logicalDecoder.getPostgresPluginName())
                             .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                             .build();
    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();

    // YB Note: Added a wait for replication slot to be active.
    TestHelper.waitFor(Duration.ofSeconds(10));

    waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
    // there shouldn't be any snapshot records
    assertNoRecordsToConsume();

    // insert and verify 2 new records
    TestHelper.execute("INSERT INTO s2.a VALUES (1, 22, 'random text value');");
    TestHelper.execute("UPDATE s2.a SET aa = 12345 WHERE pk = 1;");

    SourceRecords actualRecords = consumeRecordsByTopic(2);
    List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s2.a"));

    SourceRecord insertRecord = records.get(0);
    SourceRecord updateRecord = records.get(1);

    if (logicalDecoder.isYBOutput()) {
      YBVerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
      YBVerifyRecord.isValidUpdate(updateRecord, PK_FIELD, 1);
    } else {
      VerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
      VerifyRecord.isValidUpdate(updateRecord, PK_FIELD, 1);
    }

    Struct updateRecordValue = (Struct) updateRecord.value();
    assertThat(updateRecordValue.get(Envelope.FieldName.AFTER)).isNotNull();
    assertThat(updateRecordValue.get(Envelope.FieldName.BEFORE)).isNull();

    // After field will have entries for all the columns.
    if (logicalDecoder.isYBOutput()) {
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).getStruct("pk").getInt32("value")).isEqualTo(1);
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).getStruct("aa").getInt32("value")).isEqualTo(12345);
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).getStruct("bb").getString("value")).isEqualTo("random text value");
    } else {
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).get("pk")).isEqualTo(1);
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).get("aa")).isEqualTo(12345);
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).get("bb")).isEqualTo("random text value");
    }
  }

  @Test
  public void shouldProduceEventsWithValuesForChangedColumnWithReplicaIdentityChange() throws Exception {
    // YB Note: Note that even if we do not alter, the default replica identity on service is CHANGE.
    TestHelper.execute("ALTER TABLE s2.a REPLICA IDENTITY CHANGE;");

    Configuration config = TestHelper.defaultConfig()
                             .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                             .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                             .build();
    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();

    // YB Note: Added a wait for replication slot to be active.
    TestHelper.waitFor(Duration.ofSeconds(10));

    waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
    // there shouldn't be any snapshot records
    assertNoRecordsToConsume();

    // insert and verify 3 new records
    TestHelper.execute("INSERT INTO s2.a VALUES (1, 22, 'random text value');");
    TestHelper.execute("UPDATE s2.a SET aa = 12345 WHERE pk = 1;");
    TestHelper.execute("UPDATE s2.a SET aa = null WHERE pk = 1;");

    SourceRecords actualRecords = consumeRecordsByTopic(3);
    List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s2.a"));

    SourceRecord insertRecord = records.get(0);
    SourceRecord updateRecord = records.get(1);
    SourceRecord updateRecordWithNullCol = records.get(2);

    YBVerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
    YBVerifyRecord.isValidUpdate(updateRecord, PK_FIELD, 1);
    YBVerifyRecord.isValidUpdate(updateRecordWithNullCol, PK_FIELD, 1);

    Struct updateRecordValue = (Struct) updateRecord.value();
    assertThat(updateRecordValue.get(Envelope.FieldName.AFTER)).isNotNull();
    assertThat(updateRecordValue.get(Envelope.FieldName.BEFORE)).isNull();

    // After field will have entries for all the changed columns.
    assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).getStruct("pk").getInt32("value")).isEqualTo(1);
    assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).getStruct("aa").getInt32("value")).isEqualTo(12345);
    assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).getStruct("bb")).isNull();

    // After field will have a null value in place of the column explicitly set as null.
    Struct updateRecordWithNullColValue = (Struct) updateRecordWithNullCol.value();
    assertThat(updateRecordWithNullColValue.getStruct(Envelope.FieldName.AFTER).getStruct("pk").getInt32("value")).isEqualTo(1);
    assertThat(updateRecordWithNullColValue.getStruct(Envelope.FieldName.AFTER).getStruct("aa").getInt32("value")).isNull();
    assertThat(updateRecordWithNullColValue.getStruct(Envelope.FieldName.AFTER).getStruct("bb")).isNull();
  }

  @Test
  public void shouldThrowExceptionWithReplicaIdentityNothingOnUpdatesAndDeletes() throws Exception {
    /*
      According to Postgres docs:
      If a table without a replica identity is added to a publication that replicates
      UPDATE or DELETE operations then subsequent UPDATE or DELETE operations will cause
      an error on the publisher.

      Details: https://www.postgresql.org/docs/current/logical-replication-publication.html
     */
    TestHelper.execute("ALTER TABLE s1.a REPLICA IDENTITY NOTHING;");
    TestHelper.execute("ALTER TABLE s2.a REPLICA IDENTITY NOTHING;");

    Configuration config = TestHelper.defaultConfig()
                             .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                             .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                             .build();
    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();

    // YB Note: Added a wait for replication slot to be active.
    TestHelper.waitFor(Duration.ofSeconds(10));

    waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
    // there shouldn't be any snapshot records
    assertNoRecordsToConsume();

    // insert and verify 2 new records
    TestHelper.execute("INSERT INTO s2.a VALUES (1, 22, 'random text value');");

    try {
      TestHelper.execute("UPDATE s2.a SET aa = 12345 WHERE pk = 1;");
    } catch (Exception sqle) {
      assertThat(sqle.getMessage()).contains("ERROR: cannot update table \"a\" because it does "
                                              + "not have a replica identity and publishes updates");
    }

    try {
      TestHelper.execute("DELETE FROM s2.a WHERE pk = 1;");
    } catch (Exception sqle) {
      assertThat(sqle.getMessage()).contains("ERROR: cannot delete from table \"a\" because it "
                                              + "does not have a replica identity and publishes deletes");
    }
  }

  @Test
  public void beforeImageForDeleteWithReplicaIdentityFullAndPgOutput() throws Exception {
    shouldHaveBeforeImageForDeletesForReplicaIdentityFull(PostgresConnectorConfig.LogicalDecoder.PGOUTPUT);
  }

  @Test
  public void beforeImageForDeleteWithReplicaIdentityFullAndYbOutput() throws Exception {
    shouldHaveBeforeImageForDeletesForReplicaIdentityFull(PostgresConnectorConfig.LogicalDecoder.YBOUTPUT);
  }

  public void shouldHaveBeforeImageForDeletesForReplicaIdentityFull(PostgresConnectorConfig.LogicalDecoder logicalDecoder) throws Exception {
    TestHelper.execute("ALTER TABLE s1.a REPLICA IDENTITY FULL;");
    TestHelper.execute("ALTER TABLE s2.a REPLICA IDENTITY FULL;");
    Configuration config = TestHelper.defaultConfig()
                             .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                             .with(PostgresConnectorConfig.PLUGIN_NAME, logicalDecoder.getPostgresPluginName())
                             .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                             .build();
    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();

    // YB Note: Added a wait for replication slot to be active.
    TestHelper.waitFor(Duration.ofSeconds(10));

    waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
    // there shouldn't be any snapshot records
    assertNoRecordsToConsume();

    // insert and verify 2 new records
    TestHelper.execute("INSERT INTO s2.a VALUES (1, 22, 'random text value');");
    TestHelper.execute("DELETE FROM s2.a WHERE pk = 1;");

    SourceRecords actualRecords = consumeRecordsByTopic(2);
    List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s2.a"));

    SourceRecord insertRecord = records.get(0);
    SourceRecord deleteRecord = records.get(1);

    if (logicalDecoder.isYBOutput()) {
      YBVerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
      YBVerifyRecord.isValidDelete(deleteRecord, PK_FIELD, 1);
    } else {
      VerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
      VerifyRecord.isValidDelete(deleteRecord, PK_FIELD, 1);
    }

    Struct deleteRecordValue = (Struct) deleteRecord.value();
    assertThat(deleteRecordValue.get(Envelope.FieldName.AFTER)).isNull();
    assertThat(deleteRecordValue.get(Envelope.FieldName.BEFORE)).isNotNull();

    // Before field will have entries for all the columns.
    if (logicalDecoder.isYBOutput()) {
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("pk").getInt32("value")).isEqualTo(1);
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("aa").getInt32("value")).isEqualTo(22);
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("bb").getString("value")).isEqualTo("random text value");
    } else {
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).get("pk")).isEqualTo(1);
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).get("aa")).isEqualTo(22);
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).get("bb")).isEqualTo("random text value");
    }
  }

  @Test
  public void beforeImageForDeleteWithReplicaIdentityDefaultAndPgOutput() throws Exception {
    shouldHaveBeforeImageForDeletesForReplicaIdentityDefault(PostgresConnectorConfig.LogicalDecoder.PGOUTPUT);
  }

  @Test
  public void beforeImageForDeleteWithReplicaIdentityDefaultAndYbOutput() throws Exception {
    shouldHaveBeforeImageForDeletesForReplicaIdentityDefault(PostgresConnectorConfig.LogicalDecoder.YBOUTPUT);
  }

  public void shouldHaveBeforeImageForDeletesForReplicaIdentityDefault(PostgresConnectorConfig.LogicalDecoder logicalDecoder) throws Exception {
    TestHelper.execute("ALTER TABLE s1.a REPLICA IDENTITY DEFAULT;");
    TestHelper.execute("ALTER TABLE s2.a REPLICA IDENTITY DEFAULT;");

    Configuration config = TestHelper.defaultConfig()
                             .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                             .with(PostgresConnectorConfig.PLUGIN_NAME, logicalDecoder.getPostgresPluginName())
                             .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                             .build();
    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();

    // YB Note: Added a wait for replication slot to be active.
    TestHelper.waitFor(Duration.ofSeconds(10));

    waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
    // there shouldn't be any snapshot records
    assertNoRecordsToConsume();

    // insert and verify 2 new records
    TestHelper.execute("INSERT INTO s2.a VALUES (1, 22, 'random text value');");
    TestHelper.execute("DELETE FROM s2.a WHERE pk = 1;");

    SourceRecords actualRecords = consumeRecordsByTopic(2);
    List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s2.a"));

    SourceRecord insertRecord = records.get(0);
    SourceRecord deleteRecord = records.get(1);

    if (logicalDecoder.isYBOutput()) {
      YBVerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
      YBVerifyRecord.isValidDelete(deleteRecord, PK_FIELD, 1);
    } else {
      VerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
      VerifyRecord.isValidDelete(deleteRecord, PK_FIELD, 1);
    }

    Struct deleteRecordValue = (Struct) deleteRecord.value();
    assertThat(deleteRecordValue.get(Envelope.FieldName.AFTER)).isNull();
    assertThat(deleteRecordValue.get(Envelope.FieldName.BEFORE)).isNotNull();

    // Before field will have entries only for the primary key columns.
    if (logicalDecoder.isYBOutput()) {
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("pk").getInt32("value")).isEqualTo(1);
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("aa").getInt32("value")).isNull();
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("bb").getString("value")).isNull();
    } else {
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).get("pk")).isEqualTo(1);
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).get("aa")).isNull();
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).get("bb")).isNull();
    }
  }

  @Test
  public void shouldHaveBeforeImageForDeletesForReplicaIdentityChange() throws Exception {
    // YB Note: Note that even if we do not alter, the default replica identity on service is CHANGE.
    TestHelper.execute("ALTER TABLE s2.a REPLICA IDENTITY CHANGE;");

    Configuration config = TestHelper.defaultConfig()
                             .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                             .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                             .build();
    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();

    // YB Note: Added a wait for replication slot to be active.
    TestHelper.waitFor(Duration.ofSeconds(10));

    waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
    // there shouldn't be any snapshot records
    assertNoRecordsToConsume();

    // insert and verify 2 new records
    TestHelper.execute("INSERT INTO s2.a VALUES (1, 22, 'random text value');");
    TestHelper.execute("DELETE FROM s2.a WHERE pk = 1;");

    SourceRecords actualRecords = consumeRecordsByTopic(2);
    List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s2.a"));

    SourceRecord insertRecord = records.get(0);
    SourceRecord deleteRecord = records.get(1);

    YBVerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
    YBVerifyRecord.isValidDelete(deleteRecord, PK_FIELD, 1);

    Struct deleteRecordValue = (Struct) deleteRecord.value();
    assertThat(deleteRecordValue.get(Envelope.FieldName.AFTER)).isNull();
    assertThat(deleteRecordValue.get(Envelope.FieldName.BEFORE)).isNotNull();

    // Before field will have entries only for the primary key columns.
    assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("pk").getInt32("value")).isEqualTo(1);
    assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("aa").getInt32("value")).isNull();
    assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("bb").getString("value")).isNull();
  }


  @Test
  public void shouldFilterUpdateAndDeleteForNoPkTableWithNonFullRI() throws Exception {
    TestHelper.execute("CREATE TABLE s2.nopk (aa integer, bb varchar(20));");
    TestHelper.execute("ALTER TABLE s2.nopk REPLICA IDENTITY CHANGE;");

    TestHelper.dropPublication();
    TestHelper.execute("CREATE PUBLICATION dbz_publication FOR TABLE s2.nopk;");

    Configuration config = TestHelper.defaultConfig()
        .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
        .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
        .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE,
            PostgresConnectorConfig.AutoCreateMode.DISABLED.getValue())
        .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s2.nopk")
        .build();

    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();
    TestHelper.waitFor(Duration.ofSeconds(5));

    TestHelper.execute("INSERT INTO s2.nopk VALUES (1, 'test');");
    TestHelper.execute(
        "SET yb_cdcsdk_allow_dml_without_pk = true; "
            + "UPDATE s2.nopk SET aa = 99 WHERE aa = 1;");
    TestHelper.execute(
        "SET yb_cdcsdk_allow_dml_without_pk = true; "
            + "DELETE FROM s2.nopk WHERE aa = 99;");
    TestHelper.execute("INSERT INTO s2.nopk VALUES (2, 'after-filter');");

    SourceRecords records = consumeRecordsByTopic(2);
    List<SourceRecord> nopkRecords = records.recordsForTopic(topicName("s2.nopk"));

    assertThat(nopkRecords).hasSize(2);

    Struct firstInsertValue = (Struct) nopkRecords.get(0).value();
    Struct secondInsertValue = (Struct) nopkRecords.get(1).value();
    assertThat(firstInsertValue.getString("op")).isEqualTo(Envelope.Operation.CREATE.code());
    assertThat(firstInsertValue.getStruct(Envelope.FieldName.AFTER).getStruct("aa").getInt32("value")).isEqualTo(1);
    assertThat(secondInsertValue.getString("op")).isEqualTo(Envelope.Operation.CREATE.code());
    assertThat(secondInsertValue.getStruct(Envelope.FieldName.AFTER).getStruct("aa").getInt32("value")).isEqualTo(2);
  }

  @Test
  public void shouldNotFilterUpdateAndDeleteForNoPkTableWithFullRI() throws Exception {
    final LogInterceptor streamLog = new LogInterceptor(PostgresStreamingChangeEventSource.class);
    streamLog.setLoggerLevel(PostgresStreamingChangeEventSource.class, Level.DEBUG);

    TestHelper.execute("CREATE TABLE s2.nopk_full (aa integer, bb varchar(20));");
    TestHelper.execute("ALTER TABLE s2.nopk_full REPLICA IDENTITY FULL;");

    TestHelper.dropPublication();
    TestHelper.execute("CREATE PUBLICATION dbz_publication FOR TABLE s2.nopk_full;");

    Configuration config = TestHelper.defaultConfig()
        .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
        .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
        .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE,
            PostgresConnectorConfig.AutoCreateMode.DISABLED.getValue())
        .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s2.nopk_full")
        .build();

    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();

    Awaitility.await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(1))
        .until(() -> streamLog.containsMessage("Processing messages"));

    TestHelper.execute("INSERT INTO s2.nopk_full VALUES (1, 'test');");
    TestHelper.execute("UPDATE s2.nopk_full SET aa = 99 WHERE aa = 1;");
    TestHelper.execute("DELETE FROM s2.nopk_full WHERE aa = 99;");

    // All 3 records (INSERT, UPDATE, DELETE) should come through since RI is FULL.
    waitForAvailableRecords(30_000, TimeUnit.MILLISECONDS);
    SourceRecords records = consumeRecordsByTopic(3);
    List<SourceRecord> nopkRecords = records.recordsForTopic(topicName("s2.nopk_full"));

    assertThat(nopkRecords).hasSize(3);

    Struct insertValue = (Struct) nopkRecords.get(0).value();
    Struct updateValue = (Struct) nopkRecords.get(1).value();
    Struct deleteValue = (Struct) nopkRecords.get(2).value();

    assertThat(insertValue.getString("op")).isEqualTo(Envelope.Operation.CREATE.code());
    assertThat(updateValue.getString("op")).isEqualTo(Envelope.Operation.UPDATE.code());
    assertThat(deleteValue.getString("op")).isEqualTo(Envelope.Operation.DELETE.code());

    // Verify no filtering log was emitted.
    assertThat(streamLog.containsMessage("UPDATE/DELETE record(s) in the last 5 minutes")).isFalse();
  }

  @Test
  public void shouldBlockUpdateAndDeleteForNoPkTableWithFlagFalse() throws Exception {
    // Scenarios 1 & 2: With flag=false (default), server BLOCKS UPDATE/DELETE on
    // non-PK tables for BOTH DEFAULT and CHANGE RI. With the new server code (D51670),
    // CHANGE and DEFAULT are treated identically.

    // --- DEFAULT RI ---
    TestHelper.execute("CREATE TABLE s2.nopk_default (aa integer, bb varchar(20));");
    TestHelper.execute("ALTER TABLE s2.nopk_default REPLICA IDENTITY DEFAULT;");

    TestHelper.dropPublication();
    TestHelper.execute("CREATE PUBLICATION dbz_publication FOR TABLE s2.nopk_default;");

    TestHelper.execute("INSERT INTO s2.nopk_default VALUES (1, 'test');");

    assertThatThrownBy(() -> TestHelper.execute("UPDATE s2.nopk_default SET aa = 99 WHERE aa = 1;"))
        .hasMessageContaining("does not have a replica identity and publishes updates");

    assertThatThrownBy(() -> TestHelper.execute("DELETE FROM s2.nopk_default WHERE aa = 1;"))
        .hasMessageContaining("does not have a replica identity and publishes deletes");

    // --- CHANGE RI ---
    TestHelper.execute("CREATE TABLE s2.nopk_change_block (aa integer, bb varchar(20));");
    TestHelper.execute("ALTER TABLE s2.nopk_change_block REPLICA IDENTITY CHANGE;");

    TestHelper.dropPublication();
    TestHelper.execute("CREATE PUBLICATION dbz_publication FOR TABLE s2.nopk_change_block;");

    TestHelper.execute("INSERT INTO s2.nopk_change_block VALUES (1, 'test');");

    assertThatThrownBy(() -> TestHelper.execute("UPDATE s2.nopk_change_block SET aa = 99 WHERE aa = 1;"))
        .hasMessageContaining("does not have a replica identity and publishes updates");

    assertThatThrownBy(() -> TestHelper.execute("DELETE FROM s2.nopk_change_block WHERE aa = 1;"))
        .hasMessageContaining("does not have a replica identity and publishes deletes");
  }

  @Test
  public void shouldFilterUpdateDeleteAfterAlterToFullBecauseStreamRIIsStale() throws Exception {
    // flag=false (default) scenario:
    // Phase 1: RI=CHANGE, no PK -> server BLOCKS UPDATE/DELETE.
    // Phase 2: ALTER to FULL -> server ALLOWS UPDATE/DELETE (FULL always works).
    // But stream RI stays CHANGE (stale) -> connector FILTERS them.
    final LogInterceptor schemaLog = new LogInterceptor(PostgresSchema.class);
    final LogInterceptor streamLog = new LogInterceptor(PostgresStreamingChangeEventSource.class);
    streamLog.setLoggerLevel(PostgresStreamingChangeEventSource.class, Level.DEBUG);

    TestHelper.execute("CREATE TABLE s2.nopk_alter (aa integer, bb varchar(20));");
    TestHelper.execute("ALTER TABLE s2.nopk_alter REPLICA IDENTITY CHANGE;");

    TestHelper.dropPublication();
    TestHelper.execute("CREATE PUBLICATION dbz_publication FOR TABLE s2.nopk_alter;");

    Configuration config = TestHelper.defaultConfig()
        .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
        .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
        .with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE,
            PostgresConnectorConfig.AutoCreateMode.DISABLED.getValue())
        .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s2.nopk_alter")
        .build();

    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();

    Awaitility.await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(1))
        .until(() -> streamLog.containsMessage("Processing messages"));

    // INSERT works regardless -- confirm stream RI is CHANGE.
    TestHelper.execute("INSERT INTO s2.nopk_alter VALUES (1, 'before alter');");

    Awaitility.await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(1))
        .until(() -> schemaLog.containsMessage("Replica identity being stored for table s2.nopk_alter is CHANGE"));

    LOGGER.info("Confirmed: stream stored RI=CHANGE for nopk_alter");

    waitForAvailableRecords(30_000, TimeUnit.MILLISECONDS);
    SourceRecords insertRecords = consumeRecordsByTopic(1);
    assertThat(insertRecords.recordsForTopic(topicName("s2.nopk_alter"))).hasSize(1);

    // Phase 1: With flag=false and RI=CHANGE, server blocks UPDATE/DELETE.
    assertThatThrownBy(() -> TestHelper.execute("UPDATE s2.nopk_alter SET aa = 99 WHERE aa = 1;"))
        .hasMessageContaining("does not have a replica identity and publishes updates");

    assertThatThrownBy(() -> TestHelper.execute("DELETE FROM s2.nopk_alter WHERE aa = 1;"))
        .hasMessageContaining("does not have a replica identity and publishes deletes");

    LOGGER.info("Phase 1 confirmed: UPDATE/DELETE blocked by server with RI=CHANGE");

    // Phase 2: ALTER to FULL -- server now allows UPDATE/DELETE.
    // But the stream RI stays CHANGE (stale).
    // NOTE: On YB debug builds, mid-stream ALTER REPLICA IDENTITY bumps the
    // schema version and the next CDC poll's SchemaPackingStorage lookup hits
    // a DCHECK in schema_packing.cc that aborts the tserver. Run with
    // --TEST_dcheck_for_missing_schema_packing=false. Release builds self-heal
    // via the recovery path in cdcsdk_producer.cc (AddSchema).
    TestHelper.execute("ALTER TABLE s2.nopk_alter REPLICA IDENTITY FULL;");
    TestHelper.waitFor(Duration.ofSeconds(5));

    // DMLs go through because actual table RI is now FULL.
    TestHelper.execute("UPDATE s2.nopk_alter SET aa = 99 WHERE aa = 1;");
    TestHelper.execute("DELETE FROM s2.nopk_alter WHERE aa = 99;");

    // Connector should STILL filter because stream RI = CHANGE (stale, non-FULL).
    // The throttled summary log fires once per 5 minutes, so we wait for a single
    // emission. The "0 additional records" assertion below proves both UPDATE and
    // DELETE were filtered.
    Awaitility.await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(1))
        .until(() -> streamLog.containsMessage("UPDATE/DELETE record(s) in the last 5 minutes"));

    // Only the initial INSERT should have been dispatched after the ALTER.
    // No additional records should be available.
    assertThat(consumeAvailableRecords(record -> { })).isEqualTo(0);

    LOGGER.info("Verified: UPDATE/DELETE filtered after ALTER to FULL because stream RI is stale (CHANGE)");
  }

}
