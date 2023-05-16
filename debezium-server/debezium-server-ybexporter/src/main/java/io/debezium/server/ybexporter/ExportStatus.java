/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Singleton class that is used to update the status of the export process.
 */
public class ExportStatus {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportStatus.class);
    private static final String EXPORT_STATUS_FILE_NAME = "export_status.json";
    private static ExportStatus instance;
    private static ObjectMapper mapper = new ObjectMapper(new JsonFactory());
    private String dataDir;
    private ConcurrentMap<String, Long> sequenceMax;
    private ConcurrentMap<Table, TableExportStatus> tableExportStatusMap = new ConcurrentHashMap<>();
    private ExportMode mode;
    private ObjectWriter ow;
    private File f;
    private File tempf;


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
        f = new File(getFilePath(datadirStr));
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
        TableExportStatus tableExportStatus = new TableExportStatus(tableExportStatusMap.size(), tblFilename);
        tableExportStatusMap.put(t, tableExportStatus);
    }

    public void updateTableSnapshotRecordWritten(Table t) {
        tableExportStatusMap.get(t).exportedRowCountSnapshot++;
    }

    public void updateMode(ExportMode modeEnum) {
        mode = modeEnum;
    }

    public void setSequenceMaxMap(ConcurrentMap<String, Long> sequenceMax){
        this.sequenceMax = sequenceMax;
    }

    public ConcurrentMap<String, Long> getSequenceMaxMap(){
        return this.sequenceMax;
    }

    public synchronized void flushToDisk() {
        // TODO: do not create fresh objects every time, just reuse.
        HashMap<String, Object> exportStatusMap = new HashMap<>();
        List<HashMap<String, Object>> tablesInfo = new ArrayList<>();
        for (Map.Entry<Table, TableExportStatus> pair : tableExportStatusMap.entrySet()) {
            Table t = pair.getKey();
            TableExportStatus tes = pair.getValue();
            HashMap<String, Object> tableInfo = new HashMap<>();
            tableInfo.put("database_name", t.dbName);
            tableInfo.put("schema_name", t.schemaName);
            tableInfo.put("table_name", t.tableName);
            tableInfo.put("file_name", tes.snapshotFilename);
            tableInfo.put("exported_row_count_snapshot", tes.exportedRowCountSnapshot);
            tableInfo.put("sno", tes.sno);
            tablesInfo.add(tableInfo);
        }

        exportStatusMap.put("tables", tablesInfo);
        exportStatusMap.put("mode", mode);
        exportStatusMap.put("sequences", sequenceMax);

        try {
            // for atomic write, we write to a temp file, and then
            // rename to destination file path. This prevents readers from reading the file in a corrupted
            // state (for example, when the complete file has not been written)
            tempf = new File(getTempFilePath());
            ow.writeValue(tempf, exportStatusMap);
            Files.move(tempf.toPath(), f.toPath(), REPLACE_EXISTING);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getFilePath(String dataDirStr){
        return String.format("%s/%s", dataDirStr, EXPORT_STATUS_FILE_NAME);
    }

    private String getTempFilePath(){
        return getFilePath(dataDir) + ".tmp";
    }

    private static ExportStatus loadFromDisk(String datadirStr) {
        try {
            Path p = Paths.get(getFilePath(datadirStr));
            File f = new File(p.toUri());
            if (!f.exists()) {
                return null;
            }

            String fileContent = Files.readString(p);
            var exportStatusJson = mapper.readTree(fileContent);
            LOGGER.info("Loaded export status info from disk = {}", exportStatusJson);

            ExportStatus es = new ExportStatus(datadirStr);
            es.updateMode(ExportMode.valueOf(exportStatusJson.get("mode").asText()));

            var tablesJson = exportStatusJson.get("tables");
            for (var tableJson : tablesJson) {
                // TODO: creating a duplicate table here. it will again be created when parsing a record of the table for the first time.
                Table t = new Table(tableJson.get("database_name").asText(), tableJson.get("schema_name").asText(), tableJson.get("table_name").asText());

                TableExportStatus tes = new TableExportStatus(tableJson.get("sno").asInt(), tableJson.get("file_name").asText());
                tes.exportedRowCountSnapshot = tableJson.get("exported_row_count_snapshot").asInt();
                es.tableExportStatusMap.put(t, tes);
            }
            var sequencesJson = exportStatusJson.get("sequences");
            var sequencesIterator = sequencesJson.fields();
            ConcurrentHashMap<String, Long> sequenceMaxMap = new ConcurrentHashMap<>();
            while (sequencesIterator.hasNext()){
                var entry = sequencesIterator.next();
                sequenceMaxMap.put(entry.getKey(), Long.valueOf(entry.getValue().asText()));
            }
            es.setSequenceMaxMap(sequenceMaxMap);

            return es;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

class TableExportStatus {
    Integer sno;
    Integer exportedRowCountSnapshot;
    String snapshotFilename;

    public TableExportStatus(Integer sno, String snapshotFilename){
        this.sno = sno;
        this.snapshotFilename = snapshotFilename;
        this.exportedRowCountSnapshot = 0;
    }
}

enum ExportMode {
    SNAPSHOT,
    STREAMING,
}