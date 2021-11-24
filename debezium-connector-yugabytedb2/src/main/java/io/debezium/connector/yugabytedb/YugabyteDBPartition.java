/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.client.AsyncYBClient;
import org.yb.client.ListTablesResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;
import org.yb.master.Master;

public class YugabyteDBPartition implements Partition {
    private static final String TABLETS_PARTITION_KEY = "tabletids";

    private final String listOfTablets;

    public YugabyteDBPartition(String listOfTablets) {
        this.listOfTablets = listOfTablets;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(TABLETS_PARTITION_KEY, listOfTablets);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final YugabyteDBPartition other = (YugabyteDBPartition) obj;
        return Objects.equals(listOfTablets, other.listOfTablets);
    }

    @Override
    public int hashCode() {
        return listOfTablets.hashCode();
    }

    static class Provider implements Partition.Provider<YugabyteDBPartition> {
        private final YugabyteDBConnectorConfig connectorConfig;
        private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBPartition.class);

        Provider(YugabyteDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<YugabyteDBPartition> getPartitions() {
            // TODO: we need to get all the tablets for the YugabyteDB
            // Return set of all the tablets
            // connect and get all the tablet ids
            // return a set of YBPartition for each tablet id
//            String masterAddress = connectorConfig.masterHost() + ":" + connectorConfig.masterPort();
//            final AsyncYBClient asyncYBClient;
//            final YBClient syncClient;
//            asyncYBClient = new AsyncYBClient.AsyncYBClientBuilder(masterAddress)
//                    .defaultAdminOperationTimeoutMs(30000)
//                    .defaultOperationTimeoutMs(30000)
//                    .defaultSocketReadTimeoutMs(30000)
//                    .build();
//
//            syncClient = new YBClient(asyncYBClient);
//
//            ListTablesResponse tablesResp = null;
//            try {
//                tablesResp = syncClient.getTablesList();
//
//            String tId = "";
//            // this.connectorConfig.getTableFilters().dataCollectionFilter().isIncluded();
//
//            for (Master.ListTablesResponsePB.TableInfo tableInfo : tablesResp.getTableInfoList()) {
//                LOGGER.info("SKSK The table name is " + tableInfo.getName());
//                if (tableInfo.getName().equals("t1") &&
//                        tableInfo.getNamespace().getName().equals("yugabyte")) {
//                    tId = tableInfo.getId().toStringUtf8();
//                }
//            }
//            LOGGER.info("SKSK the table uuid is " + tId);
//            YBTable table = syncClient.openTableByUUID(tId);
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
            return Collections.singleton(new YugabyteDBPartition(connectorConfig.getLogicalName()));
        }
    }
}
