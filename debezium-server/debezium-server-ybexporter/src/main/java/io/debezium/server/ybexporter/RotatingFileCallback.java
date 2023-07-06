/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.io.FileWriter;

public interface RotatingFileCallback {
    void preRotate(FileWriter fw, int currentIndex, long currentByteCount);
}
