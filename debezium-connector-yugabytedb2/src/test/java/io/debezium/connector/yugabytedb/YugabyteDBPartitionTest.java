/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import io.debezium.connector.common.AbstractPartitionTest;

public class YugabyteDBPartitionTest extends AbstractPartitionTest<YugabyteDBPartition> {

    @Override
    protected YugabyteDBPartition createPartition1() {
        return new YugabyteDBPartition("server1");
    }

    @Override
    protected YugabyteDBPartition createPartition2() {
        return new YugabyteDBPartition("server2");
    }
}
