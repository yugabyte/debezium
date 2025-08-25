package io.debezium.connector.postgresql;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.cdcstate.CDCStateRow;
import io.debezium.connector.postgresql.cdcstate.CDCStateTable;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.data.Envelope;

public class YugabyteDBParallelSlotIT extends PostgresConnectorIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBParallelSlotIT.class);

    @Before
    public void before() {
        super.before();

        TestHelper.execute("DROP SCHEMA IF EXISTS parallel_slot CASCADE;");
        TestHelper.execute("CREATE SCHEMA parallel_slot;");
    }
    
    @Test
    public void shouldStreamRecordsFromMultiTabletTable() throws Exception {
        PostgresStreamingChangeEventSource.TEST_maintainMapForFlushedLsn = true;

        // Create the table with 4 tablets.
        TestHelper.execute("CREATE TABLE parallel_slot.test_multi_tablets (id INT PRIMARY KEY) SPLIT INTO 4 TABLETS;");

        // Start connector with the table include list.
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.SLOT_NAME, "multi_slot")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "parallel_slot.test_multi_tablets")
                .with(PostgresConnectorConfig.STREAMING_MODE, PostgresConnectorConfig.StreamingMode.PARALLEL_SLOT.getValue());

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        // Wait for replication slot to initialize and start sending data.
        TestHelper.waitFor(Duration.ofSeconds(15));

        // Insert 100 records.
        for (int i = 0; i < 100; i++) {
            TestHelper.execute("INSERT INTO parallel_slot.test_multi_tablets (id) VALUES (" + i + ");");
        }

        // Log and wait for 60 seconds to allow records to be streamed.
        Set<Integer> primaryKeys = new HashSet<>();
        
        int noMessageIterations = 0;
        while (noMessageIterations < 10) {
            int consumed = consumeAvailableRecords(record -> {
                Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
                if (after != null) {
                    primaryKeys.add(after.getInt32("id"));
                }
            });

            if (consumed == 0) {
                noMessageIterations++;
                TestHelper.waitFor(Duration.ofSeconds(2));
            } else {
                noMessageIterations = 0;
            }
        }

        LOGGER.info("Waiting for 60 seconds for 100 records to be consumed.");
        TestHelper.waitFor(Duration.ofSeconds(60));

        Map<String, Long> tabletToHashCodeMap = getTabletToHashCodeMap("parallel_slot", "test_multi_tablets");
        CDCStateTable cdcStateTable = CDCStateTable.createCdcState();

        // Since there are 4 tablets, we expect 5 entries in the cdc_state table (assuming that we have a single slot only)
        // (4 for each tablet and 1 for the replication slot i.e. dummy_id_for_replication_slot).

        for (CDCStateRow row : cdcStateTable.getCdcStateRows()) {
            if (row.getTabletId().equals("dummy_id_for_replication_slot")) {
                continue;
            }

            Long expectedLsn =  PostgresStreamingChangeEventSource.TEST_commitLsnMap.get(tabletToHashCodeMap.get(row.getTabletId()));
            Long actualLsn = row.getData().get("confirmed_flush_lsn").asLong();
            assertEquals(expectedLsn, actualLsn);
        }

        // Stop the connector.
        stopConnector();
    }

    protected Map<String, Long> getTabletToHashCodeMap(String schemaName, String tableName) {
        Map<String, Long> tabletToHashCodeMap = new HashMap<>();
        
        try (PostgresConnection connection = TestHelper.create()) {
            final String query = "SELECT tablet_id, COALESCE((('x'||encode(partition_key_start, 'hex'))::BIT(16)::INT), 0) partition_key_start_int "
                                 + "FROM yb_local_tablets WHERE ysql_schema_name = '" + schemaName + "' AND table_name = '" + tableName + "';";
            connection.query(query, resultSet -> {
                while (resultSet.next()) {
                    String tabletId = resultSet.getString("tablet_id");
                    long partitionKeyStart = resultSet.getLong("partition_key_start_int");
                    tabletToHashCodeMap.put(tabletId, partitionKeyStart);
                }
            });
        } catch (Exception e) {
            LOGGER.error("Error fetching tablet to hash code mapping", e);
        }

        return tabletToHashCodeMap;
    }
}
