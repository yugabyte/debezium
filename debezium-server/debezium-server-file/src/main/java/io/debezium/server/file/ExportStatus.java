/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class ExportStatus {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportStatus.class);
    private static ExportStatus instance;
    String dataDir;
    Map<Table, TableExportStatus> tableExportStatusMap = new HashMap<>();
    String mode;
    ObjectWriter ow;
    File f;

    private ExportStatus(String datadirStr) {
        if (instance != null) {
            throw new RuntimeException("instance already exists.");
        }
        dataDir = datadirStr;
        ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        f = new File(dataDir + "/export_status.json");
        instance = this;
    }

    // SINGLETON
    public static ExportStatus getInstance(String datadirStr) {
        if (instance != null){
            return instance;
        }
        ExportStatus instanceFromDisk = loadFromDisk(datadirStr);
        if (instanceFromDisk != null) {
            instance = instanceFromDisk;
            return instance;
        }
        return new ExportStatus(datadirStr);
    }

    public void updateTableSnapshotWriterCreated(Table t, String tblFilename) {
        TableExportStatus tableExportStatus = new TableExportStatus();
        tableExportStatus.snapshotFilename = tblFilename;
        tableExportStatus.exportedRowCountSnapshot = 0;
        tableExportStatusMap.put(t, tableExportStatus);
    }

    public void updateTableSnapshotRecordWritten(Table t) {
        tableExportStatusMap.get(t).exportedRowCountSnapshot++;
    }

    public void updateMode(String modeStr) {
        mode = modeStr;
    }

    public void flushToDisk() {
        HashMap<String, Object> exportStatusMap = new HashMap<>();
        List<HashMap<String, Object>> tablesInfo = new ArrayList<>();
        for (Table t : tableExportStatusMap.keySet()) {
            HashMap<String, Object> tableInfo = new HashMap<>();
            tableInfo.put("database_name", t.dbName);
            tableInfo.put("schema_name", t.schemaName);
            tableInfo.put("table_name", t.tableName);
            tableInfo.put("file_name", tableExportStatusMap.get(t).snapshotFilename);
            tableInfo.put("exported_row_count", tableExportStatusMap.get(t).exportedRowCountSnapshot);
            tablesInfo.add(tableInfo);
        }

        exportStatusMap.put("tables", tablesInfo);
        exportStatusMap.put("mode", mode);

        try {
            ow.writeValue(f, exportStatusMap);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static ExportStatus loadFromDisk(String datadirStr) {
        try {
            Path p = Paths.get(datadirStr + "/export_status.json");
            File f = new File(p.toUri());
            if (!f.exists()) {
                return null;
            }

            String fileContent = Files.readString(p);
            JsonFactory factory = new JsonFactory();
            ObjectMapper mapper = new ObjectMapper(factory);
            var exportStatusJson = mapper.readTree(fileContent);
            LOGGER.info("XXX export status info = {}", exportStatusJson);
            ExportStatus es = new ExportStatus(datadirStr);
            es.updateMode(exportStatusJson.get("mode").asText());

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
                es.tableExportStatusMap.put(t, tes);
            }
            return es;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

class TableExportStatus {
    Integer exportedRowCountSnapshot;
    String snapshotFilename;
}

