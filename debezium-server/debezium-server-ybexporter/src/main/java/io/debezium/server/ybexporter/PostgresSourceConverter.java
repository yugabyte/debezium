/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.sql.JDBCType;
import java.util.Properties;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

public class PostgresSourceConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSourceConverter.class);

    @Override
    public void configure(Properties props) {
        return;
        // isbnSchema = SchemaBuilder.string().name(props.getProperty("schema.name"));
    }

    @Override
    public void converterFor(RelationalColumn column,
                             ConverterRegistration<SchemaBuilder> registration) {

        JDBCType jdbcType = JDBCType.valueOf(column.jdbcType());
        switch (jdbcType) {
            case STRUCT:
            case ARRAY:
                registration.register(SchemaBuilder.string(), x -> {
                    if (x == null) {
                        return null;
                    }
                    else {
                        return x.toString();
                    }
                });
                break;

        }
        switch (column.typeName()) {
            case "money":
            case "tsvector":
            case "tsquery":
                registration.register(SchemaBuilder.string(), x -> {
                    if (x == null) {
                        return null;
                    }
                    else {
                        return x.toString();
                    }
                });
                break;
        }
    }
}
