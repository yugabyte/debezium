/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import io.debezium.data.VariableScaleDecimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;

public class DebeziumRecordProcessor implements RecordProcessor{

    private JsonConverter jsonConverter;
    public DebeziumRecordProcessor(){
        jsonConverter = new JsonConverter();
        Map<String, String> jsonConfig = Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
        jsonConverter.configure(jsonConfig, false);
    }
    @Override
    public void processRecord(Record r) {
        for (int i = 0; i < r.keyValues.size(); i++) {
            Object val = r.keyValues.get(i);
            String column = r.keyColumns.get(i);
            Object formattedVal = makeFieldValueSerializable(val, r.t.fieldSchemas.get(column));
            r.keyValues.set(i, formattedVal);
        }

        for (int i = 0; i < r.valueValues.size(); i++) {
            Object val = r.valueValues.get(i);
            String column = r.valueColumns.get(i);
            Object formattedVal = makeFieldValueSerializable(val, r.t.fieldSchemas.get(column));
            r.valueValues.set(i, formattedVal);
        }
    }
    /**
     * For certain data-types like decimals/bytes/structs, we convert them
     * to certain formats that is serializable by downstream snapshot/streaming
     * writers
     */
    private Object makeFieldValueSerializable(Object fieldValue, Field field){
        if (fieldValue == null) {
            return null;
        }
        String logicalType = field.schema().name();
        if (logicalType != null) {
            switch (logicalType) {
                case "org.apache.kafka.connect.data.Decimal":
                    return ((BigDecimal) fieldValue).toString();
                case "io.debezium.data.VariableScaleDecimal":
                    return VariableScaleDecimal.toLogical((Struct)fieldValue).toString();
            }
        }
        Schema.Type type = field.schema().type();
        switch (type){
            case BYTES:
            case STRUCT:
                return toKafkaConnectJsonConverted(fieldValue, field);
        }
        return fieldValue;
    }

    /**
     * Use the kafka connect json converter to convert it to a json friendly string
     */
    private String toKafkaConnectJsonConverted(Object fieldValue, Field f){
        String jsonFriendlyString = new String(jsonConverter.fromConnectData("test", f.schema(), fieldValue));
        if (jsonFriendlyString.length() > 0){
            if ((jsonFriendlyString.charAt(0) == '"') && (jsonFriendlyString.charAt(jsonFriendlyString.length()-1) == '"')){
                jsonFriendlyString = jsonFriendlyString.substring(1, jsonFriendlyString.length() - 1);
            }
        }
        return jsonFriendlyString;
    }
}
