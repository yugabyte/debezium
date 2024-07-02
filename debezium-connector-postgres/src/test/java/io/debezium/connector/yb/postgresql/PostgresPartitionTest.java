/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yb.postgresql;

import io.debezium.connector.common.AbstractPartitionTest;
import io.debezium.connector.yb.postgresql.PostgresPartition;

public class PostgresPartitionTest extends AbstractPartitionTest<PostgresPartition> {

    @Override
    protected PostgresPartition createPartition1() {
        return new PostgresPartition("server1", "database1");
    }

    @Override
    protected PostgresPartition createPartition2() {
        return new PostgresPartition("server2", "database1");
    }
}
