/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

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
                    .defaultAdminOperationTimeoutMs(connectorConfig.adminOperationTimeoutMs())
                    .defaultOperationTimeoutMs(connectorConfig.operationTimeoutMs())
                    .defaultSocketReadTimeoutMs(connectorConfig.socketReadTimeoutMs())
                    .numTablets(connectorConfig.maxNumTablets())
                    .sslCertFile(connectorConfig.sslRootCert())
                    .sslClientCertFiles(connectorConfig.sslClientCert(), connectorConfig.sslClientKey())
                    .build();

            syncClient = new YBClient(asyncYBClient);

            ListTablesResponse tablesResp = null;

            try {
                tablesResp = syncClient.getTablesList();
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            Set<YBPartition> partititons = new HashSet<>();
            for (MasterDdlOuterClass.ListTablesResponsePB.TableInfo tableInfo : tablesResp.getTableInfoList()) {
                LOGGER.info("SKSK The table name is " + tableInfo.getName());
                String fqlTableName = tableInfo.getNamespace().getName() + "." + tableInfo.getPgschemaName()
                        + "." + tableInfo.getName();
                TableId tableId = YugabyteDBSchema.parseWithSchema(fqlTableName, tableInfo.getPgschemaName());
                if (this.connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                    LOGGER.info("VKVK adding tableId " + tableInfo.getId().toStringUtf8() + " to get the partition");
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
