/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.file;

public interface RecordWriter {
    public void writeRecord(Record r);

    public void flush();

    public void close();
}
