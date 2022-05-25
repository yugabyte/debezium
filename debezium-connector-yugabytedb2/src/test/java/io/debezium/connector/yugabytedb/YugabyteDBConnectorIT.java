package io.debezium.connector.yugabytedb;

import static org.fest.assertions.Assertions.*;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.embedded.AbstractConnectorTest;

public class YugabyteDBConnectorIT extends AbstractConnectorTest {

    private YugabyteDBConnector connector;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @Before
    public void before() {
        initializeConnectorTestFramework();
    }

    @After
    public void after() throws Exception {
        stopConnector();
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
    }

    private void validateFieldDef(Field expected) {
        ConfigDef configDef = connector.config();
        assertThat(configDef.names()).contains(expected.name());
        ConfigDef.ConfigKey key = configDef.configKeys().get(expected.name());
        assertThat(key).isNotNull();
        assertThat(key.name).isEqualTo(expected.name());
        assertThat(key.displayName).isEqualTo(expected.displayName());
        assertThat(key.importance).isEqualTo(expected.importance());
        assertThat(key.documentation).isEqualTo(expected.description());
        assertThat(key.type).isEqualTo(expected.type());
    }

    private CompletableFuture<Void> insertRecords(int numOfRowsToBeInserted) {
        String formatInsertString = "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha', 30);";
        return CompletableFuture.runAsync(() -> {
            for (int i = 0; i < numOfRowsToBeInserted; i++) {
                TestHelper.execute(String.format(formatInsertString, i));
            }

        }).exceptionally(throwable -> {
            throw new RuntimeException(throwable);
        });
    }

    private void verifyRecordCount(int recordsCount) {
        int totalConsumedRecords = 0;
        System.out.println("Starting to consume records");
        while (totalConsumedRecords < recordsCount) {
            int consumed = super.consumeAvailableRecords(record -> {
            });
            if (consumed > 0) {
                totalConsumedRecords += consumed;
                System.out.println("Consumed " + totalConsumedRecords + " records");
            }
        }
        assertThat(totalConsumedRecords).isEqualTo(recordsCount);
    }

    @Test
    public void shouldValidateConnectorConfigDef() {
        connector = new YugabyteDBConnector();
        ConfigDef configDef = connector.config();
        assertThat(configDef).isNotNull();
        YugabyteDBConnectorConfig.ALL_FIELDS.forEach(this::validateFieldDef);
    }

    @Test
    public void shouldNotStartWithInvalidConfiguration() throws Exception {
        // use an empty configuration which should be invalid because of the lack of DB connection details
        Configuration config = Configuration.create().build();

        // we expect the engine will log at least one error, so preface it ...
        logger.info("Attempting to start the connector with an INVALID configuration, so MULTIPLE error messages & one exceptions will appear in the log");
        start(YugabyteDBConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();
    }

    // This verifies that the connector should not be running if a wrong table.include.list is provided
    @Test
    public void shouldThrowExceptionWithWrongIncludeList() throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("postgres_create_tables.ddl");
        Thread.sleep(1000);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "all_types");

        // Create another table which will not be a part of the DB stream ID
        TestHelper.execute("CREATE TABLE not_part_of_stream (id INT PRIMARY KEY);");

        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.all_types,public.not_part_of_stream", dbStreamId);

        // This should throw a DebeziumException saying the table not_part_of_stream is not a part of stream ID
        start(YugabyteDBConnector.class, configBuilder.build(), (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isInstanceOf(DebeziumException.class);

            assertThat(error.getMessage().contains("is not a part of the stream ID " + dbStreamId)).isTrue();
        });

        assertConnectorNotRunning();
    }

    // This test verifies that the connector configuration works properly even if there are tables of the same name
    // in another database
    @Test
    public void shouldWorkWithSameNameTablePresentInAnotherDatabase() throws Exception {
        System.out.println("Starting test");
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("postgres_create_tables.ddl");
        Thread.sleep(1000);
        System.out.println("executed DDL file");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");

        // Create a same table in another database
        // This is to ensure that when the yb-client returns all the tables, then the YugabyteDBConnector
        // is filtering them properly
        String createNewTableStatement = "CREATE TABLE t1 (id INT PRIMARY KEY, first_name TEXT NOT NULL, last_name VARCHAR(40), hours DOUBLE PRECISION);";
        TestHelper.createTableInSecondaryDatabase(createNewTableStatement);

        // The config builder returns a default config with the database as "postgres"
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        System.out.println("Started connector");

        int recordsCount = 10;

        insertRecords(recordsCount);

        CompletableFuture.runAsync(() -> verifyRecordCount(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
    }
}
