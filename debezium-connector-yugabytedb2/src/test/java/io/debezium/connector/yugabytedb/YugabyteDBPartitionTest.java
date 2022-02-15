/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import io.debezium.connector.common.AbstractPartitionTest;

public class YugabyteDBPartitionTest extends AbstractPartitionTest<YBPartition> {

    @Override
    protected YBPartition createPartition1() {
        return new YBPartition("server1");
    }

    @Override
    protected YBPartition createPartition2() {
        return new YBPartition("server2");
    }
}
