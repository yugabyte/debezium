package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.connector.postgresql.cdcstate.CDCStateRow;
import io.debezium.connector.postgresql.cdcstate.CDCStateTable;

public class CDCStateTableTest extends PostgresConnectorIT {
    public final static String CREATE_STMT =
        "DROP SCHEMA IF EXISTS cdc_state_schema CASCADE; " +
        "CREATE SCHEMA cdc_state_schema; " +
        "CREATE TABLE cdc_state_schema.test_cdc_state_wrapper (id INT PRIMARY KEY) SPLIT INTO 2 TABLETS;";

    @Test
    public void testCDCStateTableWhenEmpty() {
        CDCStateTable cdcStateTable = CDCStateTable.createCdcState();
        
        // Since we have not created any replication slot, the cdc_state table
        // will not even exist and hence this will be empty.
        assertThat(cdcStateTable.isEmpty()).isTrue();
    }

    @Test
    public void shouldHaveLastReplicationTimeWithoutConsumption() {
        TestHelper.execute(CREATE_STMT);
        TestHelper.createPublicationForAllTables();
        TestHelper.createDefaultReplicationSlot();

        CDCStateTable cdcStateTable = CDCStateTable.createCdcState();
        for (CDCStateRow row : cdcStateTable.getCdcStateRows()) {
            if (row.getTabletId().equals("dummy_id_for_replication_slot")) {
                assertThat(row.getLastReplicationTime()).isNull();
            } else {
                assertThat(row.getLastReplicationTime()).isNotNull();
            }
        }
    }
}
