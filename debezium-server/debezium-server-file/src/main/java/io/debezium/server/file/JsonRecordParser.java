/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.file;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonRecordParser implements RecordParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonRecordParser.class);
    Map<String, Table> tableMap;
    JsonConverter jsonConverter;

    public JsonRecordParser(Map<String, Table> tblMap) {
        tableMap = tblMap;
        jsonConverter = new JsonConverter();
        Map<String, String> jsonConfig = Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        jsonConverter.configure(jsonConfig, false);
    }

    /**
     * This deserialization operation will most likely be heavier as compared to a simple
     * jsonMapper, but this comes with the added advantages of converting the json values to
     * java native object types (for example, of type bytes to bytearray, of type bool to java boolean, etc).
     * Furthermore, there is a way in future to not deal with ser-de at all, by tweaking debezium-server
     * format.value=connect, wherein it gives a SourceRecord object directly to the ChangeConsumer.
     * Therefore, this approach is used for now.
     */
    @Override
    public Record parseRecord(Object keyObj, Object valueObj) {
        try {
            String jsonValue = valueObj.toString();
            String jsonKey = keyObj.toString();

            var r = new Record();

            // Deserialize to Connect object
            SchemaAndValue valueConnectObject = jsonConverter.toConnectData("", jsonValue.getBytes());
            Struct value = (Struct) valueConnectObject.value();

            SchemaAndValue keyConnectObject = jsonConverter.toConnectData("", jsonKey.getBytes());
            Struct key = (Struct) keyConnectObject.value();

            Struct source = value.getStruct("source");
            r.op = value.getString("op");
            r.snapshot = source.getString("snapshot");

            // Parse table/schema the first time to be able to format specific field values
            parseTable(value, source, r);

            // Parse key and values
            parseKeyFields(key, r);
            parseValueFields(value, r);

            return r;
        }
        catch (Exception ex) {
            LOGGER.error("Failed to parse msg: {}", ex);
            throw new RuntimeException(ex);
        }
    }

    protected void parseTable(Struct value, Struct sourceNode, Record r) {
        String dbName = sourceNode.getString("db");
        String schemaName = sourceNode.getString("schema");
        String tableName = sourceNode.getString("table");
        var tableIdentifier = dbName + "-" + schemaName + "-" + tableName;

        Table t = tableMap.get(tableIdentifier);
        if (t == null) {
            // create table
            t = new Table();
            t.dbName = dbName;
            t.schemaName = schemaName;
            t.tableName = tableName;

            // parse fields
            Struct afterStruct = value.getStruct("after");
            for (Field f : afterStruct.schema().fields()) {
                t.fieldSchemas.put(f.name(), f);
            }

            tableMap.put(tableIdentifier, t);
        }
        r.t = t;
    }

    protected void parseKeyFields(Struct key, Record r) {
        try {
            for (Field f : key.schema().fields()) {
                Object fieldValue = YugabyteDialectConverter.fromConnect(f, key.get(f));
                r.key.put(f.name(), fieldValue);
            }
        }
        catch (Exception ex) {
            LOGGER.error("XXX parseKey: {}", ex);
        }
    }

    /**
     * Parses value fields from the msg.
     * In case of update operation, only stores the fields that have changed by comparing
     * the before and after structs.
     */
    protected void parseValueFields(Struct value, Record r) {
        Struct before = value.getStruct("before");
        Struct after = value.getStruct("after");
        if (after == null) {
            return;
        }
        for (Field f : after.schema().fields()) {
            if (r.op.equals("u")) {
                // TODO: error handle before is NULL
                if (Objects.equals(after.get(f), before.get(f))) {
                    // no need to record this as field is unchanged
                    continue;
                }
            }
            Object fieldValue = YugabyteDialectConverter.fromConnect(f, after.get(f));
            r.fields.put(f.name(), fieldValue);
        }
    }

}
