/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

/**
 * Helper class to add server related methods to aid in code execution for YugabyteDB specific flow.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBServer {
    public static boolean isEnabled() {
        return true;
    }
}
