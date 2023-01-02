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
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
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
    public Record parseRecord(Object key, Object value) {
        try {
            String jsonValue = value.toString();
            String jsonKey = key.toString();

            JsonNode rootNode = mapper.readTree(jsonValue);

            var payload = rootNode.get("payload");

            if (payload == null || payload.isNull()) {
                LOGGER.info("XXX payload is null {}", value);
                return null;
            }
            var op = payload.get("op");
            if (op == null || op.isNull()) {
                LOGGER.info("XXX op is null {}", jsonValue);
                return null;
            }
            if (!op.asText().equals("r") && !op.asText().equals("c") && !op.asText().equals("d") && !op.asText().equals("u")) {
                LOGGER.info("XXX unknown op {}", op.asText());
                return null;
            }
            var after = payload.get("after");
            if ((after == null || after.isNull()) && (!op.asText().equals("d"))) {
                LOGGER.info("XXX after is null {}", jsonValue);
                return null;
            }
            var before = payload.get("before");
            if (op.asText().equals("u")) {
                LOGGER.info("UPDATE BEFORE FIELD = {}", before);
            }

            var source = payload.get("source");
            var r = new Record();
            r.op = op.asText();
            r.snapshot = source.get("snapshot").asText();

            // Parse table/schema the first time to be able to format specific field values
            var schemaNode = rootNode.get("schema");
            parseTable(jsonValue, schemaNode, source, r);

            // parse key in case of CDC
            parseKeyFields(jsonKey, r);

            // parse fields and construt rowText
            parseValueFields(before, after, r);

            return r;
        }
        catch (Exception ex) {
            LOGGER.error("XXX parse: {}", ex);
        }
        LOGGER.info("XXX end returning NULL {}", value);
        return null;
    }

    protected void parseTable(String jsonValue, JsonNode schemaNode, JsonNode sourceNode, Record r) {
        String dbName = sourceNode.get("db").asText();
        String schemaName = sourceNode.get("schema").asText();
        String tableName = sourceNode.get("table").asText();
        var tableIdentifier = dbName + "-" + schemaName + "-" + tableName;

        Table t = tableMap.get(tableIdentifier);
        if (t == null) {
            // create table
            t = new Table();
            t.dbName = dbName;
            t.schemaName = schemaName;
            t.tableName = tableName;

            // parse fields
            var schemaAndValue = jsonConverter.toConnectData("", jsonValue.getBytes());
            ConnectSchema schema = (ConnectSchema) schemaAndValue.schema();
            for (Field f : schema.fields()) {
                t.fields.put(f.name(), f);
            }

            tableMap.put(tableIdentifier, t);
        }
        r.ti = t;
    }

    protected void parseKeyFields(String jsonKey, Record r) {
        try {
            JsonNode rootNode = mapper.readTree(jsonKey);
            var payload = rootNode.get("payload");
            var payloadFields = payload.fields();
            while (payloadFields.hasNext()) {
                var f = payloadFields.next();
                var v = f.getValue();
                // LOGGER.info("value = {}, value_type = {}", v, v.getClass().getName());
                // var formattedValue = formatFieldValue(r.ti, f.getKey(), v.asText());
                var formattedValue = v.asText();
                r.key.put(f.getKey(), formattedValue);
            }

        }
        catch (Exception ex) {
            LOGGER.error("XXX parseKey: {}", ex);
        }
    }

    protected void parseValueFields(JsonNode before, JsonNode after, Record r) {
        if (after == null) {
            return;
        }
        var fields = after.fields();
        while (fields.hasNext()) {
            var f = fields.next();
            var v = f.getValue();
            if (r.op.equals("u")) {
                if (v.equals(before.get(f.getKey()))) {
                    // no need to record this as field is unchanged
                    continue;
                }
            }
            // LOGGER.info("value = {}, value_type = {}", v, v.getClass().getName());
            // var formattedValue = formatFieldValue(r.ti, f.getKey(), v.asText());
            var formattedValue = v.asText();
            r.fields.put(f.getKey(), formattedValue);
        }
    }

}
