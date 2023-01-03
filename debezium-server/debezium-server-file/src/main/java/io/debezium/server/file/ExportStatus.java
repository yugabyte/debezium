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

/**
 * Singleton class that is used to update the status of the export process.
 */
public class ExportStatus {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportStatus.class);
    private static final String EXPORT_STATUS_FILE_NAME = "export_status.json";
    private static ExportStatus instance;
    private String dataDir;
    private Map<Table, TableExportStatus> tableExportStatusMap = new HashMap<>();
    private ExportMode mode;
    private ObjectWriter ow;
    private File f;

    /**
     * Should only be called once in the lifetime of the process.
     * Creates and instance and assigns it to static instance property of the class.
     */
    private ExportStatus(String datadirStr) {
        //
        if (instance != null) {
            throw new RuntimeException("instance already exists.");
        }
        dataDir = datadirStr;
        ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        f = new File(String.format("%s/%s", dataDir, EXPORT_STATUS_FILE_NAME));
        instance = this;
    }

    /**
     * 1. Tries to check if an instance is already created
     * 2. If not, tries to load from disk.
     * 3. If not available on disk, creates a new instance.
     * @param datadirStr - data directory
     * @return instance
     */
    public static ExportStatus getInstance(String datadirStr) {
        if (instance != null) {
            return instance;
        }
        ExportStatus instanceFromDisk = loadFromDisk(datadirStr);
        if (instanceFromDisk != null) {
            instance = instanceFromDisk;
            return instance;
        }
        return new ExportStatus(datadirStr);
    }

    public ExportMode getMode() {
        return mode;
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

    public void updateMode(ExportMode modeEnum) {
        mode = modeEnum;
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
            LOGGER.info("Loaded export status info from disk = {}", exportStatusJson);
            ExportStatus es = new ExportStatus(datadirStr);
            String exportModeText = exportStatusJson.get("mode").asText();
            es.updateMode(ExportMode.valueOf(exportModeText));

            var tablesJson = exportStatusJson.get("tables");
            for (var tableJson : tablesJson) {
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

enum ExportMode {
    SNAPSHOT,
    STREAMING,
}