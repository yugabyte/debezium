/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.file;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    @ConfigProperty(name = PROP_PREFIX + "dataDir")
    String dataDir;

    Map<String, BufferedWriter> writers = new HashMap<>();
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
            for (var writer : writers.values()) {
                try {
                    writer.flush();
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
            LOGGER.info("{}.{}.{} => {}", r.dbName, r.schemaName, r.tableName, r.rowText);
            try {
                writeRecord(r);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            // committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }

    private void writeRecord(Record r) throws IOException {
        var fileName = dataDir + "/" + r.dbName + "-" + r.schemaName + "-" + r.tableName;
        var writer = writers.get(fileName);
        if (writer == null) {
            var f = new FileWriter(fileName);
            writer = new BufferedWriter(f);
            writers.put(fileName, writer);
        }
        writer.write(r.rowText);
        writer.write('\n');
        // writer.flush();
    }

    private void parseSchema(JsonNode schemaNode, Record r) {
        var identifier = dataDir + "/" + r.dbName + "-" + r.schemaName + "-" + r.tableName;
        var tableFieldSchema = tableFieldSchemas.get(identifier);
        if (tableFieldSchema == null) {
            var fields = schemaNode.get("fields");
            var afterNodeSchema = fields.get(1);
            var afterNodeFields = afterNodeSchema.get("fields");

            var fieldsSchemas = new HashMap<String, FieldSchema>();

            for (final JsonNode fieldSchema : afterNodeFields) {
                LOGGER.info("FIELD SCHEMA NODE {}", fieldSchema);
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
                LOGGER.info("XXX Putting at {}", fs.name);
                fieldsSchemas.put(fs.name, fs);
            }
            tableFieldSchemas.put(identifier, fieldsSchemas);
        }
    }

    private String formatFieldValue(String tableIdentifier, String field, String val) {
        FieldSchema fs = tableFieldSchemas.get(tableIdentifier).get(field);
        if (fs.className != null) {
            switch (fs.className) {
                case "io.debezium.time.Date":
                    LocalDate date = LocalDate.ofEpochDay(Long.parseLong(val));
                    return date.toString(); // default yyyy-MM-dd
                default:
                    return val;
            }
        }
        return val;
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
            r.dbName = source.get("db").asText();
            r.schemaName = source.get("schema").asText();
            r.tableName = source.get("table").asText();
            var identifier = dataDir + "/" + r.dbName + "-" + r.schemaName + "-" + r.tableName;

            var schemaNode = rootNode.get("schema");
            parseSchema(schemaNode, r);

            var fields = after.fields();
            var values = new ArrayList<String>();
            while (fields.hasNext()) {
                var f = fields.next();
                var v = f.getValue().asText();
                var formattedValue = formatFieldValue(identifier, f.getKey(), v);
                // LOGGER.info("field = {} fieldtypes={}, type ={}", f.getKey(), tableFieldSchemas.get(identifier),
                // tableFieldSchemas.get(identifier).get("first_name"));
                // .get(f.getKey()
                values.add(formattedValue);
                // .get(f.getValue()).type
            }
            var text = String.join("\t", values);
            r.rowText = text;

            return r;
        }
        catch (Exception ex) {
            LOGGER.error("XXX parse: {}", ex);
        }
        LOGGER.info("XXX end returning NULL {}", json);
        return null;
    }
}

class Record {
    String dbName, schemaName, tableName;
    String rowText;
}

class FieldSchema {
    String name;
    String type;
    String className;
}
