/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.file;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
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
    private Map<String, Table> tableMap;
    private ObjectMapper mapper = new ObjectMapper(new JsonFactory());
    private JsonConverter jsonConverter;

    public JsonRecordParser(Map<String, Table> tblMap) {
        tableMap = tblMap;
        jsonConverter = new JsonConverter();
        Map<String, String> jsonConfig = Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        jsonConverter.configure(jsonConfig, false);
    }

    @Override
    public Record parseRecord(Object keyObj, Object valueObj) {
        try {
            String jsonValue = valueObj.toString();
            String jsonKey = keyObj.toString();

            var r = new Record();

            // Deserialize to Connect object
            SchemaAndValue valueConnectObject = jsonConverter.toConnectData("", jsonValue.getBytes());
            ConnectSchema valueSchema = (ConnectSchema) valueConnectObject.schema();
            Struct value = (Struct) valueConnectObject.value();

            SchemaAndValue keyConnectObject = jsonConverter.toConnectData("", jsonKey.getBytes());
            ConnectSchema keySchema = (ConnectSchema) keyConnectObject.schema();
            Struct key = (Struct) keyConnectObject.value();

            Struct source = value.getStruct("source");

            r.op = value.getString("op");
            r.snapshot = source.getString("snapshot");

            // Parse table/schema the first time to be able to format specific field values
            parseTable(valueSchema, source, r);

            // parse key in case of CDC
            parseKeyFields(key, r);

            // parse fields and construt rowText
            parseValueFields(value, r);

            return r;
        }
        catch (Exception ex) {
            LOGGER.error("XXX parse: {}", ex);
        }
        LOGGER.info("XXX end returning NULL {}", valueObj);
        return null;
    }

    protected void parseTable(Schema valueSchema, Struct sourceNode, Record r) {
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
            t.schema = valueSchema;
            // for (Field f : schema.fields()) {
            // t.fields.put(f.name(), f);
            // }

            tableMap.put(tableIdentifier, t);
        }
        r.ti = t;
    }

    protected void parseKeyFields(Struct key, Record r) {
        try {
            for (Field f : key.schema().fields()) {
                r.key.put(f.name(), key.get(f));
            }
            // JsonNode rootNode = mapper.readTree(jsonKey);
            // var payload = rootNode.get("payload");
            // var payloadFields = payload.fields();
            // while (payloadFields.hasNext()) {
            // var f = payloadFields.next();
            // var v = f.getValue();
            // // LOGGER.info("value = {}, value_type = {}", v, v.getClass().getName());
            // // var formattedValue = formatFieldValue(r.ti, f.getKey(), v.asText());
            // var formattedValue = jsonConverter.convert;
            // r.key.put(f.getKey(), formattedValue);
            // }

        }
        catch (Exception ex) {
            LOGGER.error("XXX parseKey: {}", ex);
        }
    }

    protected void parseValueFields(Struct value, Record r) {
        Struct before = value.getStruct("before");
        Struct after = value.getStruct("after");
        if (after == null) {
            return;
        }
        for (Field f : after.schema().fields()) {
            if (r.op.equals("u")) {
                // TODO: error handle before is NULL
                if (after.get(f).equals(before.get(f))) {
                    // no need to record this as field is unchanged
                    continue;
                }
            }
            r.fields.put(f.name(), after.get(f));
        }

        // var fields = after.fields();
        // while (fields.hasNext()) {
        // var f = fields.next();
        // var v = f.getValue();
        // if (r.op.equals("u")) {
        // if (v.equals(before.get(f.getKey()))) {
        // // no need to record this as field is unchanged
        // continue;
        // }
        // }
        // // LOGGER.info("value = {}, value_type = {}", v, v.getClass().getName());
        // // var formattedValue = formatFieldValue(r.ti, f.getKey(), v.asText());
        // var formattedValue = v.asText();
        // r.fields.put(f.getKey(), formattedValue);
        // }
    }

}
