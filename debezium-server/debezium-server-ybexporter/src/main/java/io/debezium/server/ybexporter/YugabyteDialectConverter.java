/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.BitSet;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.Bits;
import io.debezium.data.geometry.Point;

public class YugabyteDialectConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDialectConverter.class);

    /**
     * Converts objects from those present in kafka connect SourceRecord object to that interpretable by Yugabyte's dialect.
     * @param field of type kafka.connect.data.Field having schema that of
     *              https://kafka.apache.org/20/javadoc/org/apache/kafka/connect/data/Schema.Type.html
     * @param fieldValue Value of field as present in kafka.connect.data.SourceRecord. For example, as java int/long/bool
     *                   or kafka.connect Struct
     */
    public static Object fromConnect(Field field, Object fieldValue) {
        // LOGGER.info("field={}", field);
        if (fieldValue == null) {
            return fieldValue;
        }
        String logicalType = field.schema().name();
        if (logicalType != null) {
            switch (logicalType) {
                case "io.debezium.time.Date":
                    LocalDate date = LocalDate.ofEpochDay(Long.valueOf((Integer) fieldValue));
                    return date.toString(); // default yyyy-MM-dd
                case "io.debezium.time.MicroTimestamp":
                    long epochMicroSeconds = (Long) fieldValue;
                    long epochSeconds = epochMicroSeconds / 1000000;
                    long nanoOffset = (epochMicroSeconds % 1000000) * 1000;
                    LocalDateTime dt = LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanoOffset), ZoneOffset.UTC);
                    return dt.toString();
                case "io.debezium.data.Bits":
                    BitSet bs = Bits.toBitSet(null, (byte[]) fieldValue);
                    StringBuilder s = new StringBuilder();
                    for (int i = bs.length() - 1; i >= 0; i--) {
                        s.append(bs.get(i) ? "1" : "0");
                    }
                    return s.toString();
                case "io.debezium.data.geometry.Point":
                    Struct ptStruct = (Struct) fieldValue;
                    double[] point = Point.parseWKBPoint(ptStruct.getBytes("wkb"));
                    return String.format("(%f,%f)", point[0], point[1]);
            }
        }
        Type type = field.schema().type();
        switch (type) {
            case BYTES:
                StringBuilder hexString = new StringBuilder();
                // TODO: move the binary array -> hex to sequelize
                hexString.append("\\x");
                for (byte b : (byte[]) fieldValue) {
                    hexString.append(String.format("%02x", b));
                }
                return hexString.toString();

        }

        return fieldValue;
    }

    /**
     * Converts objects to a string representation that can be used in a DML SQL query.
     * For example, strings are single quoted.
     */
    public static String makeSqlStatementCompatible(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String) {
            // escape single quotes
            String formattedVal = value.toString().replace("'", "''");
            // single quote strings.
            formattedVal = String.format("'%s'", formattedVal);
            return formattedVal;
        }
        return value.toString();
    }
}
