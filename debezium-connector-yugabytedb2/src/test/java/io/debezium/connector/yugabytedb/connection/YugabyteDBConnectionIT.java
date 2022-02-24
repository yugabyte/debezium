/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.jdbc.PgConnection;

import io.debezium.connector.yugabytedb.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
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
