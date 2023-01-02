/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.file;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.kafka.connect.data.Field;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
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

    private static final String NULL_STRING = "null";

    private boolean snapshotComplete = false;

    @ConfigProperty(name = PROP_PREFIX + "dataDir")
    String dataDir;

    Map<String, Table> tableMap = new HashMap<>();

    RecordParser parser;
    Map<Table, RecordWriter> snapshotWriters = new HashMap<>();
    RecordWriter cdcWriter;
    JsonFactory factory = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper(factory);

    ExportStatus exportStatus;
    // Map<Table, Integer> snapshotRowsProcessed = new HashMap<>();
    // HashMap<String, HashMap<String, FieldSchema>> tableFieldSchemas = new HashMap<>();

    @PostConstruct
    void connect() throws URISyntaxException {
        LOGGER.info("connect() called: dataDir = {}", dataDir);

        parser = new JsonRecordParser(tableMap);
        exportStatus = ExportStatus.getInstance(dataDir);
        if (exportStatus.mode != null && exportStatus.mode.equals("streaming")) {
            handleSnapshotComplete();
        }
        else {
            exportStatus.mode = "snapshot";
        }

        Thread t = new Thread(this::flush);
        t.setDaemon(true);
        t.start();
    }

    void flush() {
        LOGGER.info("XXX Started flush thread.");
        while (true) {
            // for (CSVPrinter writer : writers.values()) {
            // try {
            // writer.flush();
            // // LOGGER.info("FLUSHED to disk");
            // }
            // catch (IOException e) {
            // throw new RuntimeException(e);
            // }
            // }
            // try {
            // if (cdcWriterOld != null) {
            // cdcWriterOld.flush();
            // }
            // // TODO: doing more than flushing files to disk. maybe move this call to another thread?
            // updateExportStatus();
            // }
            // catch (IOException e) {
            // throw new RuntimeException(e);
            // }

            for (RecordWriter writer : snapshotWriters.values()) {
                writer.flush();
            }
            if (cdcWriter != null) {
                cdcWriter.flush();
            }
            if (exportStatus != null) {
                exportStatus.serializeToDisk();
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
            // LOGGER.info("key type = {}, value type = {}", objKey.getClass().getName(), objVal.getClass().getName());
            if (objVal == null) {
                // tombstone event
                // TODO: handle this better. try using the config to avoid altogether
                continue;
            }

            // PARSE
            var r = parser.parseRecord(objKey, objVal);
            LOGGER.info("{} => {}", r.getTableIdentifier(), r.getValues());

            // WRITE
            RecordWriter writer = getWriterForRecord(r);
            writer.writeRecord(r);
            // Handle snapshot->cdc transition
            if (r.snapshot.equals("last")) {
                handleSnapshotComplete();
            }

            committer.markProcessed(record);
        }
        committer.markBatchFinished();

    }

    private RecordWriter getWriterForRecord(Record r) {
        if (!snapshotComplete) {
            RecordWriter writer = snapshotWriters.get(r.t);
            if (writer == null) {
                writer = new TableSnapshotWriterCSV(dataDir, r.t);
                snapshotWriters.put(r.t, writer);
            }
            return writer;
        }
        else {
            return cdcWriter;
        }
    }

    private void handleSnapshotComplete() {
        snapshotComplete = true;
        exportStatus.mode = "streaming";
        closeSnapshotWriters();
        openCDCWriter();
    }

    private void closeSnapshotWriters() {
        for (RecordWriter writer : snapshotWriters.values()) {
            writer.close();
        }
        snapshotWriters.clear();
    }

    private void openCDCWriter() {
        cdcWriter = new CDCWriterJson(dataDir);
    }

    // private void writeRecord(Record r) throws IOException {
    // var table = r.t;
    //
    // CSVPrinter writer = writers.get(table);
    // if (writer == null) {
    // var fileName = getFullFileNameForTable(table);
    // var f = new FileWriter(fileName);
    // ArrayList<String> cols = table.getColumns();
    // // ArrayList<String> cols = new ArrayList<>(table.schema.keySet());
    // // final CSVFormat csvFormat = CSVFormat.Builder.create()
    // // .setHeader(String.join(",", cols))
    // // .setAllowMissingColumnNames(true)
    // // .
    // // .build();
    // writer = new CSVPrinter(f, CSVFormat.POSTGRESQL_CSV);
    // writers.put(table, writer);
    // // Write header
    // String header = String.join(CSVFormat.POSTGRESQL_CSV.getDelimiterString(), cols) + CSVFormat.POSTGRESQL_CSV.getRecordSeparator();
    // LOGGER.info("header = {}", header);
    // f.write(header);
    // // writer.print(header);
    // // writer.printHeaders();
    //
    // TableExportStatus tableExportStatus = new TableExportStatus();
    // tableExportStatus.fileName = getFilenameForTable(table);
    // tableExportStatus.exportedRowCountSnapshot = 0;
    // exportStatus.tableExportStatusMap.put(table, tableExportStatus);
    // }
    // writer.printRecord(r.getValues());
    // if (!snapshotComplete) {
    // exportStatus.tableExportStatusMap.get(table).exportedRowCountSnapshot++;
    // // Integer tableRowsProcessed = snapshotRowsProcessed.get(table);
    // // if (tableRowsProcessed == null) {
    // // tableRowsProcessed = 0;
    // // }
    // // snapshotRowsProcessed.put(table, tableRowsProcessed + 1);
    // }
    //
    // }

    private void updateExportStatus() {
        HashMap<String, Object> exportStatusMap = new HashMap<>();
        List<HashMap<String, Object>> tablesInfo = new ArrayList<>();
        for (Table t : exportStatus.tableExportStatusMap.keySet()) {
            HashMap<String, Object> tableInfo = new HashMap<>();
            tableInfo.put("database_name", t.dbName);
            tableInfo.put("schema_name", t.schemaName);
            tableInfo.put("table_name", t.tableName);
            tableInfo.put("file_name", exportStatus.tableExportStatusMap.get(t).snapshotFilename);
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
                tes.snapshotFilename = tableJson.get("file_name").asText();
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
    LinkedHashMap<String, Field> fieldSchemas = new LinkedHashMap<>();

    @Override
    public String toString() {
        return dbName + "-" + schemaName + "-" + tableName;
    }

    public ArrayList<String> getColumns() {
        return new ArrayList<>(fieldSchemas.keySet());
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
    Table t;
    LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
    String snapshot;
    String op;
    HashMap<String, Object> key = new HashMap<>();

    public String getTableIdentifier() {
        return t.toString();
    }

    public ArrayList<Object> getValues() {
        return new ArrayList<>(fields.values());
    }

    public HashMap<String, Object> getCDCInfo() {
        HashMap<String, Object> cdcInfo = new HashMap<>();
        cdcInfo.put("op", op);
        cdcInfo.put("schema_name", t.schemaName);
        cdcInfo.put("table_name", t.tableName);
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
    String snapshotFilename;
}
