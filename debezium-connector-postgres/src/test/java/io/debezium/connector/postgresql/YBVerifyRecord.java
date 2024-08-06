/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.VerifyRecord;

public class YBVerifyRecord extends VerifyRecord {
    public static void hasValidKey(SourceRecord record, String pkField, int pk) {
        Struct key = (Struct) record.key();
        assertThat(key.getStruct(pkField).get("value")).isEqualTo(pk);
    }

    public static void isValidRead(SourceRecord record, String pkField, int pk) {
        hasValidKey(record, pkField, pk);
        isValidRead(record);
    }

    public static void isValidInsert(SourceRecord record, String pkField, int pk) {
        hasValidKey(record, pkField, pk);
        isValidInsert(record, true);
    }

    public static void isValidUpdate(SourceRecord record, String pkField, int pk) {
        hasValidKey(record, pkField, pk);
        isValidUpdate(record, true);
    }

    public static void isValidDelete(SourceRecord record, String pkField, int pk) {
        hasValidKey(record, pkField, pk);
        isValidDelete(record, true);
    }

    public static void isValidTombstone(SourceRecord record, String pkField, int pk) {
        hasValidKey(record, pkField, pk);
        isValidTombstone(record);
    }
}
