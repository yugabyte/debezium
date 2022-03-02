/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb.connection;

import org.junit.After;

import io.debezium.util.Testing;

/**
 * Integration test for {@link YugabyteDBConnection}
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class YugabyteDBConnectionIT {

    @After
    public void after() {
        Testing.Print.disable();
    }

}
