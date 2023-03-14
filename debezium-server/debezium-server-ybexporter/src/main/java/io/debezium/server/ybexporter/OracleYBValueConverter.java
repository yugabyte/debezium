/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

public class OracleYBValueConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleYBValueConverter.class);

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
                registration.register(SchemaBuilder.string(), x -> {
                    try {
                        LOGGER.info("in oracle value converter for struct");
                        if (x == null){
                            return null;
                        }
                        else if (x instanceof Struct){
                            Struct orclStruct = (Struct) x;
                            Object[] attrs = orclStruct.getAttributes();
                            StringBuilder structRepr = new StringBuilder();
                            structRepr.append("(");
                            String arrayCombined = Arrays.stream(attrs).map(e->'"'+e.toString()+'"').collect(Collectors.joining(","));
                            structRepr.append(arrayCombined);
                            structRepr.append(")");
                            return structRepr.toString();
                        }
                        else {
                            return null;
                        }

                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
                break;

        }
    }
}