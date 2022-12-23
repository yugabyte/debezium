/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.file;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

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

    private static final String NULL_STRING = "NULL";

    private boolean snapshotComplete = false;
    Map<Table, Integer> snapshotRowsProcessed = new HashMap<>();

    @ConfigProperty(name = PROP_PREFIX + "dataDir")
    String dataDir;

    Map<Table, CSVPrinter> writers = new HashMap<Table, CSVPrinter>();
    JsonFactory factory = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper(factory);

    HashMap<String, HashMap<String, FieldSchema>> tableFieldSchemas = new HashMap<>();

    @PostConstruct
    void connect() throws URISyntaxException {
        LOGGER.info("connect() called: dataDir = {}", dataDir);
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
            // Object objKey = record.key();
            Object objVal = record.value();

            var r = parse((String) objVal);
            if (r == null) {
                LOGGER.info("XXX Skipped: {}", objVal);
                continue;
            }
            LOGGER.info("{} => {}", r.getTableIdentifier(), r.values);
            try {
                writeRecord(r);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            // committer.markProcessed(record);
            if (snapshotComplete) {
                updateExportStatus();
            }
        }
        committer.markBatchFinished();

    }

    private String getFilenameForTable(Table t) {
        return dataDir + "/" + t.toString();
    }

    private void writeRecord(Record r) throws IOException {
        var table = r.ti;
        CSVPrinter writer = writers.get(table);
        if (writer == null) {
            var fileName = getFilenameForTable(table);
            var f = new FileWriter(fileName);
            writer = new CSVPrinter(f, CSVFormat.POSTGRESQL_CSV);
            writers.put(table, writer);
        }
        writer.printRecord(r.values);
        Integer tableRowsProcessed = snapshotRowsProcessed.get(table);
        if (tableRowsProcessed == null) {
            tableRowsProcessed = 0;
        }
        snapshotRowsProcessed.put(table, tableRowsProcessed + 1);
    }

    private void parseSchema(JsonNode schemaNode, Record r) {
        var identifier = r.getTableIdentifier();
        var tableFieldSchema = tableFieldSchemas.get(identifier);
        if (tableFieldSchema == null) {
            var fields = schemaNode.get("fields");
            var afterNodeSchema = fields.get(1);
            var afterNodeFields = afterNodeSchema.get("fields");

            var fieldsSchemas = new HashMap<String, FieldSchema>();

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
            tableFieldSchemas.put(identifier, fieldsSchemas);
        }
    }

    private String formatFieldValue(String tableIdentifier, String field, String val) {
        if (val == null || val == "null") {
            return null;
        }
        FieldSchema fs = tableFieldSchemas.get(tableIdentifier).get(field);
        if (fs.className != null) {
            switch (fs.className) {
                case "io.debezium.time.Date":
                    LocalDate date = LocalDate.ofEpochDay(Long.parseLong(val));
                    return date.toString(); // default yyyy-MM-dd
                case "io.debezium.time.MicroTimestamp":
                    long epochMicroSeconds = Long.parseLong(val);
                    long epochSeconds = epochMicroSeconds / 1000000;
                    long nanoOffset = (epochMicroSeconds % 1000000) * 1000;
                    LocalDateTime dt = LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanoOffset), ZoneOffset.UTC);
                    return dt.toString();
                default:
                    return val;
            }
        }
        return val;
    }

    private void parseFields(JsonNode after, Record r) {
        var fields = after.fields();
        while (fields.hasNext()) {
            var f = fields.next();
            var v = f.getValue();
            // LOGGER.info("value = {}, value_type = {}", v, v.getClass().getName());
            var formattedValue = formatFieldValue(r.getTableIdentifier(), f.getKey(), v.asText());
            r.values.add(formattedValue);
        }
        // var text = "";
        // r.rowText = text;
    }

    public Record parse(String json) {
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
            if (!op.asText().equals("r")) {
                LOGGER.info("XXX op not r {}", json);
                return null;
            }
            var after = payload.get("after");
            if (after == null || after.isNull()) {
                LOGGER.info("XXX after is null {}", json);
                return null;
            }

            var source = payload.get("source");
            var r = new Record();
            var ti = new Table();
            ti.dbName = source.get("db").asText();
            ti.schemaName = source.get("schema").asText();
            ti.tableName = source.get("table").asText();
            r.ti = ti;
            var snapshot = source.get("snapshot").asText();
            if (snapshot.equals("last")) {
                snapshotComplete = true;
            }

            // Parse schema the first time to be able to format specific field values
            var schemaNode = rootNode.get("schema");
            parseSchema(schemaNode, r);

            // parse fields and construt rowText
            parseFields(after, r);

            return r;
        }
        catch (Exception ex) {
            LOGGER.error("XXX parse: {}", ex);
        }
        LOGGER.info("XXX end returning NULL {}", json);
        return null;
    }

    private void updateExportStatus() {
        HashMap<String, Object> exportStatus = new HashMap<>();
        List<HashMap<String, Object>> tablesInfo = new ArrayList<>();
        for (Table t : writers.keySet()) {
            HashMap<String, Object> tableInfo = new HashMap<>();
            tableInfo.put("database", t.dbName);
            tableInfo.put("schema", t.schemaName);
            tableInfo.put("name", t.tableName);
            tableInfo.put("rows", snapshotRowsProcessed.get(t));
            tablesInfo.add(tableInfo);
        }

        exportStatus.put("tables", tablesInfo);
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String tocJson = null;
        try {
            tocJson = ow.writeValueAsString(exportStatus);
            ow.writeValue(new File(dataDir + "/export_status.json"), exportStatus);
            LOGGER.info("TABLE OF CONTENTS = {}", tocJson);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

class Table {
    String dbName, schemaName, tableName;

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
    ArrayList<String> values = new ArrayList<>();

    public String getTableIdentifier() {
        return ti.toString();
    }
}

class FieldSchema {
    String name;
    String type;
    String className;
}
