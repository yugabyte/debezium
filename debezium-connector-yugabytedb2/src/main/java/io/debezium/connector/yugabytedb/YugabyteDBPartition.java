/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import static io.debezium.connector.yugabytedb.YugabyteDBSchema.PUBLIC_SCHEMA_NAME;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.client.*;
import org.yb.master.MasterDdlOuterClass;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;

public class YugabyteDBPartition implements Partition {
    @Override
    public Map<String, String> getSourcePartition() {
        throw new UnsupportedOperationException("Currently unsupported by the YugabyteDB " +
                "connector");
    }

    @Override
    public boolean equals(Object obj) {
        throw new UnsupportedOperationException("Currently unsupported by the YugabyteDB connector");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("Currently unsupported by the YugabyteDB connector");
    }

    static class Provider implements Partition.Provider<YBPartition> {
        private final YugabyteDBConnectorConfig connectorConfig;
        private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBPartition.class);

        Provider(YugabyteDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<YBPartition> getPartitions() {
            // TODO: we need to get all the tablets for the YugabyteDB
            // Return set of all the tablets
            // connect and get all the tablet ids
            // return a set of YBPartition for each tablet id
            String masterAddress = connectorConfig.masterAddresses();
            final AsyncYBClient asyncYBClient;
            final YBClient syncClient;
            asyncYBClient = new AsyncYBClient.AsyncYBClientBuilder(masterAddress)
                    .defaultAdminOperationTimeoutMs(30000)
                    .defaultOperationTimeoutMs(30000)
                    .defaultSocketReadTimeoutMs(30000)
                    .build();

            syncClient = new YBClient(asyncYBClient);

            ListTablesResponse tablesResp = null;
            try {
                tablesResp = syncClient.getTablesList();

                String tId = "";
                // this.connectorConfig.getTableFilters().dataCollectionFilter().isIncluded();

                for (MasterDdlOuterClass.ListTablesResponsePB.TableInfo tableInfo : tablesResp.getTableInfoList()) {
                    LOGGER.info("SKSK The table name is " + tableInfo.getName());
                    if (tableInfo.getName().equals("t1") &&
                            tableInfo.getNamespace().getName().equals("yugabyte")) {
                        tId = tableInfo.getId().toStringUtf8();
                    }
                }
                LOGGER.info("SKSK the table uuid is " + tId);
                YBTable table = syncClient.openTableByUUID(tId);

            }
            catch (Exception e) {
                e.printStackTrace();
            }
            Set<YBPartition> partititons = new HashSet<>();
            for (MasterDdlOuterClass.ListTablesResponsePB.TableInfo tableInfo : tablesResp.getTableInfoList()) {
                LOGGER.info("SKSK The table name is " + tableInfo.getName()); // todo vaibhav: it prints all the table names currently
                String fqlTableName = tableInfo.getNamespace().getName() + "." + "" + PUBLIC_SCHEMA_NAME
                        + "." + tableInfo.getName();
                TableId tableId = YugabyteDBSchema.parse(fqlTableName);
                if (this.connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                    LOGGER.info(
                            "VKVK adding table ID: " + tableInfo.getId() + " of table: " + tableInfo.getName() + " in namespace: " + tableInfo.getNamespace().getName());
                    String tId = tableInfo.getId().toStringUtf8();

                    try {
                        YBTable table = syncClient.openTableByUUID(tId);
                        List<LocatedTablet> tabletLocations = table.getTabletsLocations(30000);
                        int i = 0;
                        for (LocatedTablet tablet : tabletLocations) {
                            i++;
                            String tabletId = new String(tablet.getTabletId());
                            partititons.add(new YBPartition(tabletId));
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            // return Collections.singleton(new YugabyteDBPartition(connectorConfig.getLogicalName
            // ()));
            LOGGER.info("The partition being returned is " + partititons);
            return partititons;
        }
    }
}
