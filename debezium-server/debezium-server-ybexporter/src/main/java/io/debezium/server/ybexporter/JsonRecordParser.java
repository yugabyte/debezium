/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JsonRecordParser implements RecordParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonRecordParser.class);
    private Map<String, Table> tableMap;
    private JsonConverter jsonConverter;

    public JsonRecordParser(Map<String, Table> tblMap) {
        tableMap = tblMap;
        jsonConverter = new JsonConverter();
        Map<String, String> jsonConfig = Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        jsonConverter.configure(jsonConfig, false);
    }

    /**
     * Note: This uses the org.apache.kafka.connect.json.JsonConverter to convert from json
     * to KafkaConnect objects (which is what is serialized to the json that the ChangeConsumer receives)
     * This deserialization process will most likely be heavier as compared to a simple
     * json deserializer, but this comes with the added advantages of converting the json values to
     * java native object types (for example, of type bytes to bytearray, of type bool to java boolean, etc).
     * Furthermore, there is a way in future to not deal with ser-de at all, by tweaking debezium-server
     * format.value=connect, wherein it gives a KafkaConnect object directly to the ChangeConsumer.
     * Therefore, this approach is used for now.
     */
    @Override
    public Record parseRecord(Object keyObj, Object valueObj) {
        try {
            // String jsonValue = valueObj.toString();
            // String jsonKey = null;
            // if (keyObj != null) {
            // jsonKey = keyObj.toString();
            // }
            // LOGGER.debug("Parsing key={}, value={}", jsonKey, jsonValue);

            var r = new Record();

            // Deserialize to Connect object
            // SchemaAndValue valueConnectObject = jsonConverter.toConnectData("", jsonValue.getBytes());
            // Struct value = (Struct) valueConnectObject.value();
            Struct value = (Struct) ((SourceRecord) valueObj).value();

            Struct source = value.getStruct("source");
            r.op = value.getString("op");
            r.snapshot = source.getString("snapshot");

            // Parse table/schema the first time to be able to format specific field values
            parseTable(value, source, r);

            // Parse key and values
            if (keyObj != null) {
                // SchemaAndValue keyConnectObject = jsonConverter.toConnectData("", jsonKey.getBytes());
                // Struct key = (Struct) keyConnectObject.value();
                Struct key = (Struct) ((SourceRecord) valueObj).key();
                parseKeyFields(key, r);
            }
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
        String schemaName = "";
        if (sourceNode.schema().field("schema") != null) {
            schemaName = sourceNode.getString("schema");
        }
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
        for (Field f : key.schema().fields()) {
            Object fieldValue = YugabyteDialectConverter.fromConnect(f, key.get(f));
            r.addKeyField(f.name(), fieldValue);
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
            r.addValueField(f.name(), fieldValue);
        }
    }

}
