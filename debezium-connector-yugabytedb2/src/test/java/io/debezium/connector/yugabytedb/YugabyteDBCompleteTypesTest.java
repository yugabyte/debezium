package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.YugabyteYSQLContainer;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Strings;

public class YugabyteDBCompleteTypesTest extends AbstractConnectorTest {
    private static YugabyteYSQLContainer ybContainer;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        ybContainer = TestHelper.getYbContainer();
        System.out.println("Starting YB container now");
        ybContainer.start();

        System.out.println("Container Host: " + ybContainer.getHost() + " url: " + ybContainer.getMappedPort(5433));
        TestHelper.setContainerHostPort(ybContainer.getHost(), ybContainer.getMappedPort(5433));
        TestHelper.dropAllSchemas();
    }

    @Before
    public void before() {
        initializeConnectorTestFramework();
    }

    @After
    public void after() {
        stopConnector();
    }

    @AfterClass
    public static void afterClass() {
        ybContainer.close();
    }

    protected Configuration.Builder getConfigBuilder(String fullTablenameWithSchema, String dbStreamId) throws Exception {
        return TestHelper.defaultConfig()
                .with(YugabyteDBConnectorConfig.HOSTNAME, ybContainer.getHost()) // this field is required as of now
                .with(YugabyteDBConnectorConfig.PORT, ybContainer.getMappedPort(5433))
                .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.NEVER.getValue())
                .with(YugabyteDBConnectorConfig.DELETE_STREAM_ON_STOP, Boolean.TRUE)
                .with(YugabyteDBConnectorConfig.MASTER_ADDRESSES, ybContainer.getHost() + ":7100")
                .with(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST, fullTablenameWithSchema)
                .with(YugabyteDBConnectorConfig.STREAM_ID, dbStreamId);
    }

    private void consumeRecords(long recordsCount) {
        int totalConsumedRecords = 0;
        long start = System.currentTimeMillis();
        List<SourceRecord> records = new ArrayList<>();
        while (totalConsumedRecords < recordsCount) {
            int consumed = super.consumeAvailableRecords(record -> {
                System.out.println("The record being consumed is " + record);
                records.add(record);
            });
            if (consumed > 0) {
                totalConsumedRecords += consumed;
                System.out.println("Consumed " + totalConsumedRecords + " records");
            }
        }
        System.out.println("Total duration to ingest '" + recordsCount + "' records: " +
                Strings.duration(System.currentTimeMillis() - start));

        if (records.size() != recordsCount) {
            throw new DebeziumException("Record count doesn't match");
        }

        System.out.println("After exception section");
        /*
         * // todo: make these assertions inside a for loop
         * // At this point of time, it is assumed that the list has only one record, so it is safe to get the record at index 0.
         * SourceRecord record = records.get(0);
         * System.out.println("Starting assertions");
         * assertValueField(record, "after/id/value", 404);
         * assertValueField(record, "after/bigintcol/value", 123456);
         * assertValueField(record, "after/bitcol/value", "11011");
         * System.out.println("Asserted till bitcol");
         * assertValueField(record, "after/varbitcol/value", "10101");
         * assertValueField(record, "after/booleanval/value", false);
         * assertValueField(record, "after/byteaval/value", "\\x01");
         * assertValueField(record, "after/ch/value", "five5");
         * assertValueField(record, "after/vchar/value", "sample_text");
         * assertValueField(record, "after/cidrval/value", "10.1.0.0/16");
         * assertValueField(record, "after/dt/value", 19047);
         * assertValueField(record, "after/dp/value", 12.345);
         * assertValueField(record, "after/inetval/value", "127.0.0.1");
         * assertValueField(record, "after/intervalval/value", 2505600000000L);
         * assertValueField(record, "after/jsonval/value", "{\"a\":\"b\"}");
         * assertValueField(record, "after/jsonbval/value", "{\"a\": \"b\"}");
         * assertValueField(record, "after/mc/value", "2c:54:91:88:c9:e3");
         * assertValueField(record, "after/mc8/value", "22:00:5c:03:55:08:01:02");
         * assertValueField(record, "after/mn/value", 100.50);
         * assertValueField(record, "after/nm/value", 12.34);
         * assertValueField(record, "after/rl/value", 32.145);
         * assertValueField(record, "after/si/value", 12);
         * assertValueField(record, "after/i4r/value", "[2,10)");
         * assertValueField(record, "after/i8r/value", "[101,200)");
         * assertValueField(record, "after/nr/value", "(10.45,21.32)");
         * assertValueField(record, "after/tsr/value", "(\"1970-01-01 00:00:00\",\"2000-01-01 12:00:00\")");
         * assertValueField(record, "after/tstzr/value", "(\"2017-07-04 12:30:30+00\",\"2021-07-04 07:00:30+00\")");
         * assertValueField(record, "after/dr/value", "[2019-10-08,2021-10-07)");
         * assertValueField(record, "after/txt/value", "text to verify behaviour");
         * assertValueField(record, "after/tm/value", 46052000);
         * assertValueField(record, "after/tmtz/value", "06:30:00Z");
         * assertValueField(record, "after/ts/value", 1637841600000L);
         * assertValueField(record, "after/tstz/value", "2021-11-25T06:30:00Z");
         * assertValueField(record, "after/uuidval/value", "ffffffff-ffff-ffff-ffff-ffffffffffff");
         */
    }

    @Test
    public void verifyAllWorkingDataTypesInSingleTable() throws Exception {
        System.out.println("Starting the test");

        TestHelper.dropAllSchemas();
        TestHelper.executeDDL(ybContainer, "postgres_create_tables.ddl");
        Thread.sleep(1000);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "all_types");
        System.out.println("DB Stream Id is " + dbStreamId);

        Configuration.Builder configBuilder = getConfigBuilder("public.all_types", dbStreamId);
        start(YugabyteDBConnector.class, configBuilder.build());

        assertConnectorIsRunning();

        final long recordsCount = 1;

        // This insert statement will insert a row containing all types
        TestHelper.execute(HelperStrings.INSERT_ALL_TYPES);
        System.out.println("Data inserted to the table");

        CompletableFuture.runAsync(() -> consumeRecords(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
    }
}
