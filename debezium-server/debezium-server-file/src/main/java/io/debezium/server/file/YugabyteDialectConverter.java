/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.file;

import org.apache.kafka.connect.data.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class YugabyteDialectConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDialectConverter.class);

    public static Object fromConnect(Field field, Object fieldValue) {
        LOGGER.info("field={}", field);
        String logicalType = field.schema().name();

         switch (logicalType) {
             case "io.debezium.time.Date":
                 LocalDate date = LocalDate.ofEpochDay((Long) fieldValue);
                 String dateStr = date.toString(); // default yyyy-MM-dd
                 return dateStr;
             case "io.debezium.time.MicroTimestamp":
                 long epochMicroSeconds = (Long) fieldValue;
                 long epochSeconds = epochMicroSeconds / 1000000;
                 long nanoOffset = (epochMicroSeconds % 1000000) * 1000;
                 LocalDateTime dt = LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanoOffset), ZoneOffset.UTC);
                 String dateTimeStr = dt.toString();
             return dateTimeStr;
         }
        return fieldValue;
    }
}
