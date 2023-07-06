/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.BitSet;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import io.debezium.data.Bits;
import io.debezium.data.VariableScaleDecimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KafkaConnectRecordParser implements RecordParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectRecordParser.class);
    private final ExportStatus es;
    String dataDirStr;
    private Map<String, Table> tableMap;
    private JsonConverter jsonConverter;
    Record r = new Record();

    public KafkaConnectRecordParser(String dataDirStr, Map<String, Table> tblMap) {
        this.dataDirStr = dataDirStr;
        es = ExportStatus.getInstance(dataDirStr);
        tableMap = tblMap;
        jsonConverter = new JsonConverter();
        Map<String, String> jsonConfig = Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
        jsonConverter.configure(jsonConfig, false);
    }

    /**
     * This parser parses a kafka connect SourceRecord object to a Record object
     * that contains the relevant field schemas and values.
     */
    @Override
    public Record parseRecord(Object keyObj, Object valueObj) {
        try {
            r.clear();

            // Deserialize to Connect object
            Struct value = (Struct) ((SourceRecord) valueObj).value();
            Struct key = (Struct) ((SourceRecord) valueObj).key();

            if (value == null) {
                // Ideally, we should have config tombstones.on.delete=false. In case that is not set correctly,
                // we will get those events where value field = null. Skipping those events.
                LOGGER.warn("Empty value field in event. Assuming tombstone event. Skipping - {}", valueObj);
                r.op = "unsupported";
                return r;
            }
            Struct source = value.getStruct("source");
            r.op = value.getString("op");
            r.snapshot = source.getString("snapshot");

            // Parse table/schema the first time to be able to format specific field values
            parseTable(value, source, r);

            // Parse key and values
            if (key != null) {
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
            t = new Table(dbName, schemaName, tableName);

            // parse fields
            Struct structWithAllFields = value.getStruct("after");
            if (structWithAllFields == null) {
                // in case of delete events the after field is empty, and the before field is populated.
                structWithAllFields = value.getStruct("before");
            }
            for (Field f : structWithAllFields.schema().fields()) {
                t.fieldSchemas.put(f.name(), f);
            }

            tableMap.put(tableIdentifier, t);
            es.updateTableSchema(t);
        }
        r.t = t;
    }

    protected void parseKeyFields(Struct key, Record r) {
        for (Field f : key.schema().fields()) {
            Object fieldValue = key.get(f);
//            Object fieldValue = YugabyteDialectConverter.fromConnect(f, key.get(f));
            r.addKeyField(f.name(), fieldValue);

        }
    }

    /**
     * Parses value fields from the msg.
     * In case of update operation, only stores the fields that have changed by comparing
     * the before and after structs.
     */
    protected void parseValueFields(Struct value, Record r) {
        Struct after = value.getStruct("after");
        if (after == null) {
            return;
        }
        for (Field f : after.schema().fields()) {
            if (r.op.equals("u")) {
                // TODO: error handle before is NULL
                Struct before = value.getStruct("before");
                if (Objects.equals(after.get(f), before.get(f))) {
                    // no need to record this as field is unchanged
                    continue;
                }
            }
            Object fieldValue = after.get(f);
//            Object fieldValue = YugabyteDialectConverter.fromConnect(f, after.get(f));
            // formatting
            if (fieldValue!=null){
                LOGGER.info("field={}, fieldValue={}, fieldValueType={}", f.name(), fieldValue, fieldValue.getClass().getName());
            }
            fieldValue = serialize(fieldValue, f);
            r.addValueField(f.name(), fieldValue);

        }
    }

    private Object serialize(Object fieldValue, Field field){
//        String logicalType = field.schema().name();
//        if (logicalType != null) {
//            switch (logicalType) {
//                case "io.debezium.data.Bits":
//                    ByteBuffer bb = ByteBuffer.wrap((byte[])fieldValue);
//                    bb.order(ByteOrder.BIG_ENDIAN);
//                    return toKafkaConnectJsonConverted(bb, field);
////                case "org.apache.kafka.connect.data.Decimal":
////                    return ((BigDecimal) fieldValue).toString();
//            }
//        }
        Schema.Type type = field.schema().type();
        switch (type){
            case BYTES:
            case STRUCT:
//                String jsonFriendlyString = new String(jsonConverter.fromConnectData("test", f.schema(), fieldValue));
//                LOGGER.info("field={}, fieldValue={}, fieldValueType={}, jsonFriendlyString={}", f.name(), fieldValue, fieldValue.getClass().getName(), jsonFriendlyString);
                return toKafkaConnectJsonConverted(fieldValue, field);
        }
        return fieldValue;
    }
    private Object toKafkaConnectJsonConverted(Object fieldValue, Field f){
        String jsonFriendlyString = new String(jsonConverter.fromConnectData("test", f.schema(), fieldValue));
//        LOGGER.info("field={}, fieldValue={}, fieldValueType={}, jsonFriendlyString={}", f.name(), fieldValue, fieldValue.getClass().getName(), jsonFriendlyString);
        if ((jsonFriendlyString.charAt(0) == '"') && (jsonFriendlyString.charAt(0) == jsonFriendlyString.charAt(jsonFriendlyString.length()-1))){
            jsonFriendlyString = jsonFriendlyString.substring(1, jsonFriendlyString.length() - 1);
        }
        return jsonFriendlyString;
    }

}
