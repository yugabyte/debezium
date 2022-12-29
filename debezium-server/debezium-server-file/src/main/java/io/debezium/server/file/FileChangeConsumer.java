/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.file;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import io.confluent.connect.jdbc.dialect.PostgreSqlDatabaseDialect;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;

/**
 * Implementation of the consumer that delivers the messages to an HTTP Webhook destination.
 *
 * @author Chris Baumbauer
 */
@Named("file")
@Dependent
public class FileChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.filesink.";

    private static final String NULL_STRING = "null";

    private boolean snapshotComplete = false;

    @ConfigProperty(name = PROP_PREFIX + "dataDir")
    String dataDir;

    Map<Table, CSVPrinter> writers = new HashMap<Table, CSVPrinter>();
    BufferedWriter cdcWriter;
    JsonFactory factory = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper(factory);

    Map<String, Table> tableMap = new HashMap<>();
    ExportStatus exportStatus = new ExportStatus();
    // Map<Table, Integer> snapshotRowsProcessed = new HashMap<>();
    // HashMap<String, HashMap<String, FieldSchema>> tableFieldSchemas = new HashMap<>();

    @PostConstruct
    void connect() throws URISyntaxException {
        LOGGER.info("connect() called: dataDir = {}", dataDir);
        exportStatus.mode = "snapshot";
        loadExportStatus();

        Thread t = new Thread(this::flush);
        t.setDaemon(true);
        t.start();
    }

    void flush() {
        LOGGER.info("XXX Started flush thread.");
        while (true) {
            for (CSVPrinter writer : writers.values()) {
                try {
                    writer.flush();
                    // LOGGER.info("FLUSHED to disk");
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                if (cdcWriter != null) {
                    cdcWriter.flush();
                }
                // TODO: doing more than flushing files to disk. maybe move this call to another thread?
                updateExportStatus();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            try {
                Thread.sleep(2000);
            }
            catch (InterruptedException e) {
                // Noop.
            }
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        LOGGER.info("RECEIVED BATCH IN FILE SINK" + "Size of records-" + records.size());
        for (ChangeEvent<Object, Object> record : records) {
            Object objKey = record.key();
            Object objVal = record.value();
            if (objVal == null) {
                // tombstone event
                // TODO: handle this better. try using the config to avoid altogether
                continue;
            }
            LOGGER.info("key type = {}, value type = {}", objKey.getClass().getName(), objVal.getClass().getName());

            var r = parse((String) objVal, (String) objKey);
            if (r == null) {
                LOGGER.info("XXX Skipped: {}:{}", objKey, objVal);
                continue;
            }
            LOGGER.info("{} => {}", r.getTableIdentifier(), r.getValues());
            try {
                if (!snapshotComplete) {
                    writeRecord(r);
                    if (r.snapshot.equals("last")) {
                        handleSnapshotComplete();
                    }
                }
                else {
                    // LOGGER.info("XXX Received CDC JSON key {}", objKey);
                    // LOGGER.info("XXX Received CDC JSON value {}", objVal);
                    writeRecordCDC(r);
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            committer.markProcessed(record);
        }
        committer.markBatchFinished();

    }

    private void handleSnapshotComplete() {
        snapshotComplete = true;
        exportStatus.mode = "streaming";
        closeSnapshotWriters();
        openCDCWriter();
    }

    private void closeSnapshotWriters() {
        // for each snapshot writer, close the file
        for (CSVPrinter writer : writers.values()) {
            try {
                String eof = "\\.";
                writer.getOut().append(eof);
                writer.println();
                writer.println();
                writer.close(true);
                LOGGER.info("Closing file = {}", writer.getOut().toString());
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        writers.clear();
    }

    private void openCDCWriter() {
        var fileName = dataDir + "/queue.json";
        try {
            var f = new FileWriter(fileName, true);
            cdcWriter = new BufferedWriter(f);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getFilenameForTable(Table t) {
        return t.tableName + "_data.sql";
    }

    private String getFullFileNameForTable(Table t) {
        return dataDir + "/" + getFilenameForTable(t);
    }

    private void writeRecord(Record r) throws IOException {
        var table = r.ti;

        CSVPrinter writer = writers.get(table);
        if (writer == null) {
            var fileName = getFullFileNameForTable(table);
            var f = new FileWriter(fileName);
            ArrayList<String> cols = new ArrayList<>(table.schema.keySet());
            // final CSVFormat csvFormat = CSVFormat.Builder.create()
            // .setHeader(String.join(",", cols))
            // .setAllowMissingColumnNames(true)
            // .
            // .build();
            writer = new CSVPrinter(f, CSVFormat.POSTGRESQL_CSV);
            writers.put(table, writer);
            // Write header
            String header = String.join(CSVFormat.POSTGRESQL_CSV.getDelimiterString(), cols) + CSVFormat.POSTGRESQL_CSV.getRecordSeparator();
            LOGGER.info("header = {}", header);
            f.write(header);
            // writer.print(header);
            // writer.printHeaders();

            TableExportStatus tableExportStatus = new TableExportStatus();
            tableExportStatus.fileName = getFilenameForTable(table);
            tableExportStatus.exportedRowCountSnapshot = 0;
            exportStatus.tableExportStatusMap.put(table, tableExportStatus);
        }
        writer.printRecord(r.getValues());
        if (!snapshotComplete) {
            exportStatus.tableExportStatusMap.get(table).exportedRowCountSnapshot++;
            // Integer tableRowsProcessed = snapshotRowsProcessed.get(table);
            // if (tableRowsProcessed == null) {
            // tableRowsProcessed = 0;
            // }
            // snapshotRowsProcessed.put(table, tableRowsProcessed + 1);
        }

    }

    private void writeRecordCDC(Record r) throws IOException {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String cdcJson = ow.writeValueAsString(r.getCDCInfo());
        LOGGER.info("XXX CDC json = {}", cdcJson);

        cdcWriter.write(cdcJson);
        cdcWriter.write("\n");
    }

    private void parseSchema(JsonNode schemaNode, JsonNode sourceNode, Record r) {
        // Retrieve/create table
        String dbName = sourceNode.get("db").asText();
        String schemaName = sourceNode.get("schema").asText();
        String tableName = sourceNode.get("table").asText();
        var tableIdentifier = dbName + "-" + schemaName + "-" + tableName;
        Table t;
        t = tableMap.get(tableIdentifier);
        if (t == null) {
            // create table
            t = new Table();
            t.dbName = dbName;
            t.schemaName = schemaName;
            t.tableName = tableName;

            // parse schema
            var fields = schemaNode.get("fields");
            var afterNodeSchema = fields.get(1);
            var afterNodeFields = afterNodeSchema.get("fields");

            var fieldsSchemas = new LinkedHashMap<String, FieldSchema>();

            for (final JsonNode fieldSchema : afterNodeFields) {
                var fs = new FieldSchema();
                fs.type = fieldSchema.get("type").asText();
                fs.name = fieldSchema.get("field").asText();
                var className = fieldSchema.get("name");
                if (className != null) {
                    fs.className = className.asText();
                }
                else {
                    fs.className = null;
                }
                fieldsSchemas.put(fs.name, fs);
            }
            t.schema = fieldsSchemas;

            tableMap.put(tableIdentifier, t);
        }
        r.ti = t;

        // var identifier = r.getTableIdentifier();
        // var tableFieldSchema = tableFieldSchemas.get(identifier);
        // if (tableFieldSchema == null) {
        // var fields = schemaNode.get("fields");
        // var afterNodeSchema = fields.get(1);
        // var afterNodeFields = afterNodeSchema.get("fields");
        //
        // var fieldsSchemas = new HashMap<String, FieldSchema>();
        //
        // for (final JsonNode fieldSchema : afterNodeFields) {
        // var fs = new FieldSchema();
        // fs.type = fieldSchema.get("type").asText();
        // fs.name = fieldSchema.get("field").asText();
        // var className = fieldSchema.get("name");
        // if (className != null) {
        // fs.className = className.asText();
        // }
        // else {
        // fs.className = null;
        // }
        // fieldsSchemas.put(fs.name, fs);
        // }
        // tableFieldSchemas.put(identifier, fieldsSchemas);
        // }
    }

    private String formatFieldValue(Table t, String field, String val) {
        // TODO: clean this function up. Ideal situation: have one formatting for both snapshot and cdc.
        if (val == null || val == "null") {
            return snapshotComplete ? NULL_STRING : null;
        }
        FieldSchema fs = t.schema.get(field);
        if (fs.type.equals("string")) {
            return snapshotComplete ? String.format("'%s'", val.replace("'", "''")) : val;
        }
        if (fs.className != null) {
            switch (fs.className) {
                case "io.debezium.time.Date":
                    LocalDate date = LocalDate.ofEpochDay(Long.parseLong(val));
                    String dateStr = date.toString(); // default yyyy-MM-dd
                    return snapshotComplete ? String.format("'%s'", dateStr) : dateStr;
                case "io.debezium.time.MicroTimestamp":
                    long epochMicroSeconds = Long.parseLong(val);
                    long epochSeconds = epochMicroSeconds / 1000000;
                    long nanoOffset = (epochMicroSeconds % 1000000) * 1000;
                    LocalDateTime dt = LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanoOffset), ZoneOffset.UTC);
                    String dateTimeStr = dt.toString();
                    return snapshotComplete ? String.format("'%s'", dateTimeStr) : dateTimeStr;
                default:
                    return val;
            }
        }
        return val;
    }

    private void parseFields(JsonNode before, JsonNode after, Record r) {
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
            var formattedValue = formatFieldValue(r.ti, f.getKey(), v.asText());
            r.fields.put(f.getKey(), formattedValue);
        }
        // var text = "";
        // r.rowText = text;
    }

    private void parseKey(String jsonKey, Record r) {
        try {
            JsonNode rootNode = mapper.readTree(jsonKey);
            var payload = rootNode.get("payload");
            var payloadFields = payload.fields();
            while (payloadFields.hasNext()) {
                var f = payloadFields.next();
                var v = f.getValue();
                // LOGGER.info("value = {}, value_type = {}", v, v.getClass().getName());
                var formattedValue = formatFieldValue(r.ti, f.getKey(), v.asText());
                r.key.put(f.getKey(), formattedValue);
            }

        }
        catch (Exception ex) {
            LOGGER.error("XXX parseKey: {}", ex);
        }

    }

    public Record parse(String json, String jsonKey) {
        try {
            JsonNode rootNode = mapper.readTree(json);
            // LOGGER.info("XXX Received JSON {}", json);

            var payload = rootNode.get("payload");
            // LOGGER.info("XXX Received Payload {}", payload.toPrettyString());

            if (payload == null || payload.isNull()) {
                LOGGER.info("XXX payload is null {}", json);
                return null;
            }
            var op = payload.get("op");
            if (op == null || op.isNull()) {
                LOGGER.info("XXX op is null {}", json);
                return null;
            }
            if (!op.asText().equals("r") && !op.asText().equals("c") && !op.asText().equals("d") && !op.asText().equals("u")) {
                LOGGER.info("XXX unknown op {}", op.asText());
                return null;
            }
            // if (op.asText() == "d") {
            // LOGGER.info("DELETE EVENT key = {}", jsonKey);
            // LOGGER.info("DELETE EVENT value = {}", json);
            // }
            var after = payload.get("after");
            if ((after == null || after.isNull()) && (!op.asText().equals("d"))) {
                LOGGER.info("XXX after is null {}", json);
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
            // var ti = new Table();
            // ti.dbName = source.get("db").asText();
            // ti.schemaName = source.get("schema").asText();
            // ti.tableName = source.get("table").asText();
            // r.ti = ti;
            //

            // Parse schema the first time to be able to format specific field values
            var schemaNode = rootNode.get("schema");
            parseSchema(schemaNode, source, r);

            // parse key in case of CDC
            parseKey(jsonKey, r);

            // parse fields and construt rowText
            parseFields(before, after, r);

            if (!r.op.equals("r")) {
                parseNew(json, jsonKey);
            }

            return r;
        }
        catch (Exception ex) {
            LOGGER.error("XXX parse: {}", ex);
        }
        LOGGER.info("XXX end returning NULL {}", json);
        return null;
    }

    public void parseNew(String json, String jsonKey) {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/dvdrental", "postgres", "");

            Map<String, String> connProps = new HashMap<>();
            connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
            connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
            connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:postgresql://something");

            JdbcSourceConnectorConfig cfg = new JdbcSourceConnectorConfig(connProps);
            PostgreSqlDatabaseDialect pgdialect = new PostgreSqlDatabaseDialect(cfg);

            // ALTERNATIVE WAY TO PARSE
            JsonConverter jsonConverter = new JsonConverter();
            Map<String, String> jsonConfig = Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
            jsonConverter.configure(jsonConfig, false);
            var schemaAndValue = jsonConverter.toConnectData("", json.getBytes());
            ConnectSchema schema = (ConnectSchema) schemaAndValue.schema();
            Struct value = (Struct) schemaAndValue.value();

            LOGGER.info("XXX CONVERTER schema={}{}\n value = {}{}", schemaAndValue.schema().getClass().getName(), schemaAndValue.schema(),
                    schemaAndValue.value().getClass().getName(), schemaAndValue.value());
            Struct afterStruct = value.getStruct("after");
            LinkedHashMap<String, Object> afterFields = new LinkedHashMap<>();

            if (afterStruct != null) {
                String queryString = "UPDATE customer SET";
                for (Field f : afterStruct.schema().fields()) {
                    queryString = String.format("%s %s=?,", queryString, f.name());
                }
                queryString = String.format("%s WHERE customer_id=524;", queryString);
                PreparedStatement p = conn.prepareStatement(queryString);

                int index = 1;
                for (Field f : afterStruct.schema().fields()) {
                    Schema fieldSchema = f.schema();
                    Object fieldVal = afterStruct.get(f);

                    pgdialect.bindField(p, index++, fieldSchema, fieldVal);

                    // afterFields.put(f.name(), afterStruct.get(f));
                    // LOGGER.info("XXX CONVERTER field={}|{} value={}|{}", f.name(), f.schema().name(),
                    // (afterStruct.get(f) != null) ? afterStruct.get(f).getClass().getName() : "null", afterStruct.get(f));
                    // LOGGER.info("XXX CONVERTER field={}|{} value={}|{}", f.name(), f.schema().name(), afterStruct.get(f).getClass().getName(), afterStruct.get(f));
                }
                LOGGER.info("XXX STMT = {}", p.toString());
            }

            LOGGER.info("XXX CONVERTER after = {}", value.get("after"));

        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private void updateExportStatus() {
        HashMap<String, Object> exportStatusMap = new HashMap<>();
        List<HashMap<String, Object>> tablesInfo = new ArrayList<>();
        for (Table t : exportStatus.tableExportStatusMap.keySet()) {
            HashMap<String, Object> tableInfo = new HashMap<>();
            tableInfo.put("database_name", t.dbName);
            tableInfo.put("schema_name", t.schemaName);
            tableInfo.put("table_name", t.tableName);
            tableInfo.put("file_name", exportStatus.tableExportStatusMap.get(t).fileName);
            tableInfo.put("exported_row_count", exportStatus.tableExportStatusMap.get(t).exportedRowCountSnapshot);
            tablesInfo.add(tableInfo);
        }

        exportStatusMap.put("tables", tablesInfo);
        exportStatusMap.put("mode", exportStatus.mode);
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String tocJson = null;
        try {
            tocJson = ow.writeValueAsString(exportStatusMap);
            ow.writeValue(new File(dataDir + "/export_status.json"), exportStatusMap);
            // LOGGER.info("TABLE OF CONTENTS = {}", tocJson);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void loadExportStatus() {
        try {
            Path p = Paths.get(dataDir + "/export_status.json");
            File f = new File(p.toUri());
            if (!f.exists()) {
                return;
            }

            String fileContent = Files.readString(p);
            var exportStatusJson = mapper.readTree(fileContent);
            LOGGER.info("XXX export status info = {}", exportStatusJson);
            exportStatus.mode = exportStatusJson.get("mode").asText();

            var tablesJson = exportStatusJson.get("tables");
            for (var tableJson : tablesJson) {
                LOGGER.info("XXX table info = {}", tableJson);
                // {"database_name":"dvdrental","file_name":"customer_data.sql","exported_row_count":603,"schema_name":"public","table_name":"customer"}
                // TODO: creating a duplicate table here. it will again be created when parsing a record of the table for the first time.
                Table t = new Table();
                t.dbName = tableJson.get("database_name").asText();
                t.schemaName = tableJson.get("schema_name").asText();
                t.tableName = tableJson.get("table_name").asText();

                TableExportStatus tes = new TableExportStatus();
                tes.exportedRowCountSnapshot = tableJson.get("exported_row_count").asInt();
                tes.fileName = tableJson.get("file_name").asText();
                exportStatus.tableExportStatusMap.put(t, tes);
            }
            if (exportStatus.mode.equals("streaming")) {
                handleSnapshotComplete();
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

class Table {
    String dbName, schemaName, tableName;
    LinkedHashMap<String, FieldSchema> schema = new LinkedHashMap<>();

    @Override
    public String toString() {
        return dbName + "-" + schemaName + "-" + tableName;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Table)) {
            return false;
        }
        Table t = (Table) o;
        return dbName.equals(t.dbName)
                && schemaName.equals(t.schemaName)
                && tableName.equals(t.tableName);
    }
}

class Record {
    // String dbName, schemaName, tableName;
    Table ti;
    String rowText;
    // ArrayList<String> values = new ArrayList<>();
    LinkedHashMap<String, String> fields = new LinkedHashMap<>();
    LinkedHashMap<String, Object> objFields = new LinkedHashMap<>();
    String snapshot;
    String op;
    HashMap<String, String> key = new HashMap<>();

    public String getTableIdentifier() {
        return ti.toString();
    }

    public ArrayList<String> getValues() {
        return new ArrayList<>(fields.values());
    }

    public ArrayList<Object> getObjValues() {
        return new ArrayList<>(objFields.values());
    }

    public HashMap<String, Object> getCDCInfo() {
        HashMap<String, Object> cdcInfo = new HashMap<>();
        cdcInfo.put("op", op);
        cdcInfo.put("schema_name", ti.schemaName);
        cdcInfo.put("table_name", ti.tableName);
        cdcInfo.put("key", key);
        cdcInfo.put("fields", fields);
        return cdcInfo;
    }
}

class FieldSchema {
    String name;
    String type;
    String className;
}

class TableExportStatus {
    Integer exportedRowCountSnapshot;
    String fileName;
}

class ExportStatus {
    Map<Table, TableExportStatus> tableExportStatusMap = new HashMap<>();
    String mode;
}
