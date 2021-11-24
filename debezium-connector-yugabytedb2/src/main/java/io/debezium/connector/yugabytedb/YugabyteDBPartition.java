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

        Provider(YugabyteDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<YugabyteDBPartition> getPartitions() {
            // TODO: we need to get all the tablets for the YugabyteDB
            // Return set of all the tablets
            return Collections.singleton(new YugabyteDBPartition(connectorConfig.getLogicalName()));
        }
    }
}
